from datetime import datetime
import logging
import os
import fsspec
import fsspec.utils
import pandas as pd
from typing import Any, List, Dict, Tuple

from pipeline.conformance_processor import run
from pipeline.feed_config import get_feed_config, FeedConfig
from common.cohort_membership import read_cohort_members
from common.filesystem import delete_file
import boto3
import json

# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")  # default to INFO
logger.setLevel(log_level.upper())

def lambda_handler(event, context):
    """
    AWS Lambda handler for GP pipeline processing
    
    Args:
        event: Lambda event data with parameters:
               - input_path: S3 path to input file
               - output_path: S3 path for output directory
               - feed_type: Type of feed ('gp' or 'sft')
        context: Lambda context object
        
    Returns:
        dict: Response with status code and body
    """
    response = {}

    try:
        logger.info("Starting pipeline execution")
        logger.info(f"Event: {json.dumps(event, default=str)}")

        input_path = event.get("input_path")
        output_path = event.get("output_path")
        feed_type = event.get("feed_type", "").lower()

        if not input_path:
            raise ValueError("Missing required parameter 'input_path' in event")
        if not output_path:
            raise ValueError("Missing required parameter 'output_path' in event")
        if not feed_type:
            raise ValueError("Missing required parameter 'feed_type' in event")

        feed_config = get_feed_config(feed_type)

        cohort_store_location = os.getenv("COHORT_STORE")
        pseudo_service_name = os.getenv("PSEUDONYMISATION_LAMBDA_FUNCTION_NAME")

        if not cohort_store_location:
            raise ValueError("Missing required environment variable: COHORT_STORE")
        if not pseudo_service_name:
            raise ValueError("Missing required environment variable: PSEUDONYMISATION_LAMBDA_FUNCTION_NAME")

        logger.info(
            f"Processing {feed_config.feed_type.upper()} feed")

        metadata_rows, records = _read_records(input_path, feed_config.metadata_rows_to_skip)
        cohort_store = read_cohort_members(cohort_store_location)

        cohort_member_records = run(cohort_store, records, _encrypt, feed_config)
        logger.debug(f"Processed {len(records)} records, filtered to {len(cohort_member_records)} records.")

        output_file = _write_output(cohort_member_records, metadata_rows, output_path, input_path, feed_config)
        delete_file(input_path)

        msg = f"{feed_config.feed_type.upper()} pipeline executed successfully"
        logger.info(msg)

        response = _get_response(
            request_id=context.aws_request_id,
            message=msg,
            status_code=200,
            feed_type=feed_config.feed_type,
            records_processed=len(records),
            records_retained=len(cohort_member_records),
            output_file=output_file
        )

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)

        response = _get_response(
            message=f"Pipeline execution failed: {str(e)}",
            request_id=context.aws_request_id,
            status_code=500
        )

    return response


def _read_records(path: str, skiprows: int) -> Tuple[List[str], pd.DataFrame]:
    with fsspec.open(path, mode="r", encoding="utf-8") as file:
        if isinstance(file, list):
            raise ValueError(f"Expected one file, got {len(file)}: {path}")

        lines = file.readlines()
        metadata_rows = [line.strip() for line in lines[:skiprows]] if skiprows > 0 else []

        file.seek(0)

        df = pd.read_csv(
            file,
            dtype=str,
            keep_default_na=False,
            na_values=['', 'NULL', 'null', 'None'],
            skiprows=skiprows,
            header=0
        )

    return metadata_rows, df


def _write_output(
        cohort_member_records: pd.DataFrame,
        metadata_rows: List[str],
        output_location: str,
        input_path: str,
        feed_config: FeedConfig
) -> str | None:
    original_filename = input_path.split('/')[-1]
    output_file = _write_records(cohort_member_records, metadata_rows, output_location, original_filename, feed_config)
    logger.info(f"Wrote {len(cohort_member_records)} records to {output_file}")
    return output_file


def _write_records(
        records: pd.DataFrame,
        metadata_rows: List[str],
        location: str,
        original_filename: str,
        feed_config: FeedConfig
) -> str:
    parent_dir = _get_output_dir(location, feed_config.feed_type)
    if not parent_dir:
        raise IOError(f"Failed to determine output directory under location: {location}")

    file_path = f"{parent_dir}/{original_filename}"

    logger.info(f"Writing {len(records)} records to: {file_path}")

    try:
        with fsspec.open(file_path,
                         mode="w",
                         s3_additional_kwargs={
                             "ServerSideEncryption": "aws:kms",
                             "SSEKMSKeyId": os.getenv("KMS_KEY_ID")
                         }
                         ) as file:
            if isinstance(file, list):
                raise ValueError(f"Expected single file handle, got list: {file_path}")

            if feed_config.preserve_metadata and metadata_rows:
                for metadata_row in metadata_rows:
                    file.write(metadata_row + '\n')

            if not records.empty:
                records.to_csv(file, index=False, header=True)
            else:
                file.write(','.join(records.columns) + '\n')

        logger.info(f"Successfully wrote {len(records)} records to {file_path}")
    except Exception as e:
        msg = f"Failed to write records to {file_path}: {e}"
        logger.error(msg, exc_info=True)
        raise IOError(msg)

    return file_path


def _get_output_dir(location: str, feed_type: str) -> str | None:
    try:
        protocol = fsspec.utils.get_protocol(location)
        fs = fsspec.filesystem(protocol)

        if not fs.exists(location):
            raise IOError(f"Output location {location} does not exist.")

        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        path = f"{location.rstrip('/')}/{feed_type}_feed/{year}/{month}/{day}"

        logging.info(f"Creating directory structure at {path} using fsspec filesystem for protocol {protocol}")
        fs.makedirs(path, exist_ok=True)
        logging.info(f"Created directory structure at {path}")

        return path

    except Exception as e:
        msg = f"Failed to generate output directory path under {location}: {e}"
        logging.error(msg)
        raise IOError(msg)


def _get_response(message: str, request_id: str, status_code: int, **kwargs) -> Dict[str, Any]:
    body_data = {
        'message': message,
        'requestId': request_id
    }
    body_data.update(kwargs)

    return {
        'statusCode': status_code,
        'body': json.dumps(body_data)
    }


def _encrypt(field_name: str, values: List[str]) -> List[str | None] | None:
    skip_encryption = os.getenv("SKIP_ENCRYPTION")

    if skip_encryption:
        logger.info(f"Skipping encryption for field: {field_name}")
        return values

    if not field_name:
        logger.warning(f"Field name is None or empty, cannot encrypt")
        return None

    if not values or len(values) == 0:
        logger.warning(f"Values list is None or empty for field: {field_name}")
        return None

    # Filter out invalid values and track their positions
    result = []
    invalid_count = 0

    for v in values:
        if v and str(v).lower() not in ['nan', 'none', 'null', '']:
            result.append(v)
        else:
            result.append(None)
            invalid_count += 1

    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} invalid values in list for field: {field_name}")

    # Get only valid values for encryption
    valid_values = [v for v in result if v is not None]

    if not valid_values:
        logger.warning(f"No valid values in list for field: {field_name}, returning None for all")
        return result

    # Encrypt valid values
    chunk_size = int(os.getenv("PSEUDONYMISATION_BATCH_SIZE", "10000"))
    encrypted_valid = _encrypt_batch(field_name, valid_values, chunk_size)

    # Replace valid values with encrypted ones
    encrypted_iter = iter(encrypted_valid)
    for i in range(len(result)):
        if result[i] is not None:
            result[i] = next(encrypted_iter)

    logger.info(f"Successfully encrypted {len(valid_values)} valid values out of {len(values)} total for field: {field_name}")
    return result


def _encrypt_batch(field_name: str, values: List[str], chunk_size: int) -> List[str]:
    """Encrypt values in batches if needed."""
    if len(values) <= chunk_size:
        return _encrypt_chunk(field_name, values)

    logger.info(f"Large batch detected ({len(values)} values). Processing in chunks of {chunk_size}")
    encrypted = []
    total_chunks = (len(values) + chunk_size - 1) // chunk_size

    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        logger.info(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} values)")

        encrypted_chunk = _encrypt_chunk(field_name, chunk)
        if encrypted_chunk is None:
            msg = f"Encryption failed for chunk {chunk_num}"
            logger.error(msg)
            raise ValueError(msg)

        encrypted.extend(encrypted_chunk)

    logger.info(f"Successfully processed all {total_chunks} chunks ({len(encrypted)} total encrypted values)")
    return encrypted


def _encrypt_chunk(field_name: str, values: List[str]) -> List[str] | None:
    logger.info(f"Batch encrypting {len(values)} values for field: {field_name}")

    function_name = os.getenv("PSEUDONYMISATION_LAMBDA_FUNCTION_NAME")
    if not function_name:
        msg = "Unable to resolve Pseudonymisation service. PSEUDONYMISATION_LAMBDA_FUNCTION_NAME environment variable is not set"
        logger.error(msg)
        raise ValueError(msg)

    try:
        lambda_client = boto3.client('lambda')
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'action': 'encrypt',
                'field_name': field_name,
                'field_value': values
            })
        )

        result = json.loads(response['Payload'].read())
        if 'error' in result:
            msg = f"Encryption service returned error: {result['error']}"
            logger.error(msg)
            raise ValueError(msg)
        elif 'errorMessage' in result:
            msg = f"Lambda execution error: {result['errorMessage']}"
            logger.error(msg)
            raise ValueError(msg)
        elif 'field_value' not in result:
            msg = f"Encryption service returned malformed response: missing 'field_value' for field '{field_name}'. Response: {result}"
            logger.error(msg)
            raise ValueError(msg)

        encrypted_values = result['field_value']

        if not isinstance(encrypted_values, list) or len(encrypted_values) != len(values):
            msg = f"Encryption service returned unexpected format: expected list of {len(values)} values"
            logger.error(f"{msg}, got {type(encrypted_values)}")
            raise ValueError(msg)

        logger.info(f"Successfully batch encrypted {len(values)} values for field: {field_name}")
        return encrypted_values

    except ValueError as e:
        raise
    except Exception as e:
        logger.error(f"Exception occurred while batch encrypting values for field: {field_name}: {str(e)}",
                     exc_info=True)
        raise
