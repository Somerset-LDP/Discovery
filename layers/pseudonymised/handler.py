import json
import logging
from datetime import datetime
from io import StringIO
from typing import Dict, Any

import pandas as pd

from aws_utils import (
    list_s3_files,
    read_s3_file,
    write_to_s3,
    delete_s3_file,
    invoke_pseudonymisation_lambda_batch
)
from env_utils import get_env_variables
from feed_config import FeedConfig, get_feed_config
from validation_utils import validate_dataframe


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter())
        logger.addHandler(handler)
    else:
        for handler in logger.handlers:
            handler.setFormatter(logging.Formatter())

    return logger


logger = setup_logging()


def read_csv_from_s3(bucket: str, key: str, skiprows: int, preserve_metadata: bool) -> tuple[pd.DataFrame, list[str]]:
    try:
        content = read_s3_file(bucket, key)
        content_str = content.decode('utf-8')

        lines = content_str.split('\n')
        metadata_lines = []

        if preserve_metadata and skiprows > 0:
            metadata_lines = lines[:skiprows] if len(lines) >= skiprows else []

        df = pd.read_csv(
            StringIO(content_str),
            dtype=str,
            keep_default_na=False,
            na_values=['', 'NULL', 'null', 'None'],
            skiprows=skiprows,
            header=0
        )
        return df, metadata_lines

    except UnicodeDecodeError as e:
        logger.error(f"Failed to decode file s3://{bucket}/{key}: {e}", exc_info=True)
        raise ValueError(f"File encoding error for s3://{bucket}/{key}: {str(e)}")
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV file s3://{bucket}/{key}: {e}", exc_info=True)
        raise ValueError(f"CSV parsing error for s3://{bucket}/{key}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error reading file s3://{bucket}/{key}: {e}", exc_info=True)
        raise


def normalize_nhs_numbers(df: pd.DataFrame, fields_to_pseudonymise: Dict[str, str]) -> pd.DataFrame:
    nhs_column = next((col for col, field_type in fields_to_pseudonymise.items() if field_type == 'nhs_number'), None)
    if nhs_column and nhs_column in df.columns:
        df[nhs_column] = df[nhs_column].astype(str).str.replace(' ', '', regex=False).str.strip()
    return df


def pseudonymise(df: pd.DataFrame, lambda_function_name: str, fields_to_pseudonymise: Dict[str, str]) -> pd.DataFrame:
    try:
        for csv_field_name, internal_field_name in fields_to_pseudonymise.items():
            logger.info(f"Pseudonymising column: {csv_field_name}")
            original_values = df[csv_field_name].tolist()
            pseudonymised_values = invoke_pseudonymisation_lambda_batch(
                internal_field_name,
                original_values,
                lambda_function_name
            )

            if len(pseudonymised_values) != len(original_values):
                error_msg = f"Record count mismatch for field {csv_field_name}: expected {len(original_values)}, got {len(pseudonymised_values)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            df[csv_field_name] = pseudonymised_values

        return df

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error during pseudonymisation: {e}", exc_info=True)
        raise ValueError(f"Pseudonymisation failed: {str(e)}")


def write_pseudonymised_data(
        df: pd.DataFrame,
        bucket: str,
        kms_key_id: str,
        metadata_lines: list[str],
        preserve_metadata: bool,
        feed_type: str
) -> None:
    if df.empty:
        raise ValueError("No records to write")

    output_key = generate_output_key(feed_type)
    csv_buffer = StringIO()

    if preserve_metadata:
        for line in metadata_lines:
            csv_buffer.write(line + '\n')

    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    write_to_s3(bucket, output_key, csv_content, kms_key_id)

    logger.info(f"Successfully wrote {len(df)} records to s3://{bucket}/{output_key}")


def generate_output_key(feed_type: str) -> str:
    current_date = datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    timestamp = current_date.strftime("%Y%m%d_%H%M%S_%f")
    filename = f"patient_{timestamp}.csv"
    output_key = f"{feed_type}_feed/{year}/{month}/{day}/raw/{filename}"

    return output_key


def create_response(message: str, status_code: int, **kwargs) -> Dict[str, Any]:
    body_data = {
        'message': message,
    }
    body_data.update(kwargs)
    return {
        'statusCode': status_code,
        'body': json.dumps(body_data, default=str)
    }


def process_file(
        bucket: str,
        s3_key: str,
        output_bucket: str,
        lambda_function_name: str,
        kms_key_id: str,
        feed_config: FeedConfig
) -> Dict[str, Any]:
    logger.info(f"Processing file: s3://{bucket}/{s3_key}")

    df, metadata = read_csv_from_s3(bucket, s3_key, feed_config.metadata_rows_to_skip, feed_config.preserve_metadata)
    if df.empty:
        logger.warning(f"No records found in file: s3://{bucket}/{s3_key}")
        delete_s3_file(bucket, s3_key)
        return {
            'records_input': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'records_pseudonymised': 0
        }

    records_input = len(df)
    logger.info(f"File {s3_key}: {records_input} records on input")

    df, invalid_records = validate_dataframe(df, feed_config.validation_rules, feed_config.fields_to_pseudonymise)
    records_valid = len(df)
    records_invalid = len(invalid_records)

    logger.info(
        f"File {s3_key}: {records_valid} valid records after validation, {records_invalid} invalid records removed")

    if df.empty:
        logger.warning(f"No valid records remaining in file: s3://{bucket}/{s3_key} after validation")
        delete_s3_file(bucket, s3_key)
        return {
            'records_input': records_input,
            'records_valid': records_valid,
            'records_invalid': records_invalid,
            'records_pseudonymised': 0
        }

    df = normalize_nhs_numbers(df, feed_config.fields_to_pseudonymise)
    df = pseudonymise(df, lambda_function_name, feed_config.fields_to_pseudonymise)
    records_pseudonymised = len(df)

    logger.info(f"File {s3_key}: {records_pseudonymised} records after pseudonymisation")

    if records_pseudonymised != records_valid:
        error_msg = f"File {s3_key}: Record count mismatch after pseudonymisation."
        logger.error(error_msg)
        raise ValueError(error_msg)

    write_pseudonymised_data(
        df,
        output_bucket,
        kms_key_id,
        metadata,
        feed_config.preserve_metadata,
        feed_config.feed_type
    )

    delete_s3_file(bucket, s3_key)

    logger.info(
        f"Successfully processed file s3://{bucket}/{s3_key}.")

    return {
        'records_input': records_input,
        'records_valid': records_valid,
        'records_invalid': records_invalid,
        'records_pseudonymised': records_pseudonymised
    }


def process_all_files(
        input_bucket: str,
        input_prefix: str,
        output_bucket: str,
        lambda_function_name: str,
        kms_key_id: str,
        feed_config: FeedConfig
) -> Dict[str, Any]:
    files = list_s3_files(input_bucket, input_prefix)

    processed_files = []
    total_records_input = 0
    total_records_valid = 0
    total_records_invalid = 0
    total_records_pseudonymised = 0

    if files:
        for s3_key in files:
            processed_file = process_file(
                input_bucket,
                s3_key,
                output_bucket,
                lambda_function_name,
                kms_key_id,
                feed_config
            )

            processed_files.append(processed_file)
            total_records_input += processed_file['records_input']
            total_records_valid += processed_file['records_valid']
            total_records_invalid += processed_file['records_invalid']
            total_records_pseudonymised += processed_file['records_pseudonymised']
    else:
        logger.warning(f"No files to process in s3://{input_bucket}/{input_prefix}")

    return {
        'files_processed': len(processed_files),
        'total_records_input': total_records_input,
        'total_records_valid': total_records_valid,
        'total_records_invalid': total_records_invalid,
        'total_records_pseudonymised': total_records_pseudonymised
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        logger.info("Starting pseudonymisation pipeline execution")
        env_vars = get_env_variables()
        lambda_function_name = env_vars['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME']
        kms_key_id = env_vars['KMS_KEY_ID']

        input_bucket = event.get("input_s3_bucket")
        input_prefix = event.get("input_prefix")
        output_bucket = event.get("output_s3_bucket")
        feed_type = event.get("feed_type", "").lower()

        if not all([input_bucket, input_prefix, output_bucket, feed_type]):
            raise ValueError("Missing required parameter in event")

        feed_config = get_feed_config(feed_type)

        summary = process_all_files(
            input_bucket,
            input_prefix,
            output_bucket,
            lambda_function_name,
            kms_key_id,
            feed_config
        )

        if summary['files_processed'] == 0:
            message = "No CSV files found to process"
            logger.warning(message)
        else:
            message = "Pseudonymisation pipeline executed successfully"
            logger.info(f"{message}, Summary: {summary}")

        return create_response(
            message=message,
            status_code=200,
            **summary
        )

    except Exception as e:
        logger.error(f"Pseudonymisation pipeline execution failed: {e}", exc_info=True)
        return create_response(
            message=f"Pseudonymisation pipeline execution failed: {str(e)}",
            status_code=500
        )
