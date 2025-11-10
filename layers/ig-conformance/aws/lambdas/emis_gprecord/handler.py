from datetime import datetime
import json
import logging
import os
import fsspec
import fsspec.utils
import pandas as pd
from typing import Any, List, Dict, Tuple
from pipeline.emis_gprecord import run
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
        event: Lambda event data (could contain S3 event, manual trigger, etc.)
        context: Lambda context object
        
    Returns:
        dict: Response with status code and body
    """
    response = {}

    try:
        logger.info("Starting GP pipeline execution")
        logger.info(f"Event: {json.dumps(event, default=str)}")

        # read the cohort store
        cohort_store_location = os.getenv("COHORT_STORE")
        gp_records_store_location = os.getenv("INPUT_LOCATION")
        output_location = os.getenv("OUTPUT_LOCATION")
        pseudo_service_name = os.getenv("PSEUDONYMISATION_LAMBDA_FUNCTION_NAME")
        if cohort_store_location and gp_records_store_location and output_location and pseudo_service_name:
            metadata_rows, gp_records = _read_gp_records(gp_records_store_location)
            cohort_store = read_cohort_members(cohort_store_location)

            cohort_member_records = run(cohort_store, gp_records, _encrypt)
            logger.debug(f"Processed {len(gp_records)} records, filtered to {len(cohort_member_records)} records.")

            # TODO - need to keep first three rows
            output_file = _write_output(cohort_member_records, metadata_rows, output_location)
            delete_file(gp_records_store_location)

            logger.info("GP pipeline executed successfully")

            response = _get_response(
                message='GP pipeline executed successfully',
                request_id=context.aws_request_id,
                status_code=200,
                records_processed=len(gp_records),
                records_retained=len(cohort_member_records),
                output_file=output_file
            )
    
        else:
            response = _get_response(
                message='Missing one or more of the required environment variables - COHORT_STORE, INPUT_LOCATION, OUTPUT_LOCATION, PSEUDONYMISATION_LAMBDA_FUNCTION_NAME',
                request_id=context.aws_request_id,
                status_code=400
            )

    except Exception as e:
        logger.error(f"GP pipeline execution failed: {str(e)}", exc_info=True)
        
        response = _get_response(
            message=f"GP pipeline execution failed: {str(e)}",
            request_id=context.aws_request_id,
            status_code=500
        )

    return response

# File system methods

def _read_gp_records(path: str) -> Tuple[List[str], pd.DataFrame]:
    with fsspec.open(path, mode="r", encoding="utf-8") as file:
        if isinstance(file, list):
            raise ValueError(f"Expected one file, got {len(file)}: {path}")
        
        # Read the first 2 lines as headers
        lines = file.readlines()
        metadata_rows = [line.strip() for line in lines[:2]]
        
        # Reset file pointer and read as DataFrame
        file.seek(0)        

        # Read CSV with all columns as strings to preserve leading zeros and handle data consistently
        df = pd.read_csv(
            file, 
            dtype=str, 
            keep_default_na=False, 
            na_values=['', 'NULL', 'null', 'None'],  
            skiprows=2, # skip metadata rows
            header=0 # Single header row
        )

    return metadata_rows, df

def _write_gp_records(records: pd.DataFrame, metadata_rows: List[str], location: str) -> str:
    """
    Write GP records to CSV file in the same format as input
    
    Args:
        records: List of record dictionaries to write
        location: Output file location (local path or S3 URL)
    """
    parent_dir = _get_output_dir(location)
    if not parent_dir:
        raise IOError(f"Failed to determine output directory under location: {location}")
    
    file_path = _get_output_file(parent_dir, "gp_records")

    if not file_path:
        raise IOError(f"Failed to determine output file path under location: {location}")

    logger.info(f"Writing {len(records)} records to: {file_path}")

    fs = fsspec.filesystem(fsspec.utils.get_protocol(file_path),     
                           s3_additional_kwargs={
                                "ServerSideEncryption": "aws:kms",
                                "SSEKMSKeyId": os.getenv("KMS_KEY_ID")
    })

    try:
        with fs.open(file_path, 
                         mode="w", 
                         encoding="utf-8"
        ) as file:
            # Check if file is a list (shouldn't happen with single file path, but fsspec can be unpredictable)
            if isinstance(file, list):
                raise ValueError(f"Expected single file handle, got list: {file_path}")

            # Write the original metadata rows
            for metadata_row in metadata_rows:
                file.write(metadata_row + '\n')
            
            # Write the filtered data
            if not records.empty:
                records.to_csv(file, index=False, header=True)
            else:
                # If no records, still write column headers
                file.write(','.join(records.columns) + '\n')
        
        logger.info(f"Successfully wrote {len(records)} records to {file_path}")
    except Exception as e:
        logger.error(f"Failed to write GP records to {file_path}: {str(e)}", exc_info=True)
        raise IOError(f"Failed to write GP records to {file_path}: {str(e)}")    

    return file_path

def _write_output(cohort_member_records, metadata_rows, output_location) -> str | None:
    # Write the filtered results to output file
    output_file = _write_gp_records(cohort_member_records, metadata_rows, output_location)
    logger.info(f"Wrote {len(cohort_member_records)} records to {output_file}")

    return output_file

def _get_output_dir(location: str) -> str | None:
    """
    Get the parent directory path from the given location using fsspec for universal storage support.

    Args:
        location: Output directory location (file://path, s3://bucket/path, az://container/path, etc.)

    Returns:
        str: Full path for output directory
    """
    path = None

    try:
        protocol = fsspec.utils.get_protocol(location)
        fs = fsspec.filesystem(protocol)

        if not fs.exists(location):
            raise IOError(f"Output location {location} does not exist.")

        # Generate date/time components
        current_date = datetime.now()
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        path = f"{location.rstrip('/')}/{year}/{month}/{day}" 

        logging.info(f"Creating directory structure at {path} using fsspec filesystem for protocol {protocol}")
        try:
            fs.makedirs(path, exist_ok=True)
            logging.info(f"Created directory structure at {path}")

        except (Exception) as e:
            logging.info(f"Failed to create directory structure at {path}: {e}")
            logging.error(f"Failed to create directory structure at {path}: {e}")
            raise IOError(f"Failed to create directory structure at {path}: {e}")                   
        
    except Exception as e:
        logging.error(f"Error creating output directory path under {location}: {e}")
        raise IOError(f"Failed to generate output directory path under {location}: {e}")

    return path

def _get_output_file(dir_path: str, file_name_prefix: str) -> str | None:
    """
    Get the output file path from the given location using fsspec for universal storage support.

    Args:
        location: Output directory location (file://path, s3://bucket/path, az://container/path, etc.)

    Returns:
        str: Full path for output file
    """
    file_path = None

    try:
        protocol = fsspec.utils.get_protocol(dir_path)
        fs = fsspec.filesystem(protocol)

        logging.info(f"Creating output file under {dir_path} using fsspec filesystem for protocol {protocol}")

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"gp_records_{timestamp}.csv"
        file_path = f"{dir_path}/{filename}"

        try:
            fs.touch(file_path, exist_ok=True)
            logging.info(f"Generated output file path: {file_path}")
        except Exception as e:
            logging.info(f"Failed to create output file at {file_path}: {e}")
            logging.error(f"Failed to create output file at {file_path}: {e}")
            raise IOError(f"Failed to create output file at {file_path}: {e}") 
    except Exception as e:
        logging.error(f"Error creating output file at {file_path}: {e}")
        raise IOError(f"Failed to generate output file at {file_path}: {e}")

    return file_path

# HTTP response helpers

def _get_response(message: str, request_id: str, status_code: int, **kwargs) -> Dict[str, Any]:
    """
    Helper function to create a standardized error response.
    
    Args:
        message: Error message
        request_id: AWS Lambda request ID
        status_code: HTTP status code (default is 400)
        
    Returns:
        dict: Standardized error response
    """
    body_data = {
        'message': message,
        'requestId': request_id
    }
    
    # Add any additional key-value pairs
    body_data.update(kwargs)
 
    return {
        'statusCode': status_code,
        'body': json.dumps(body_data)
    }

# This function deliberately does not include 
# - network/timeout handling - Network issues not handled
# - retry logic - Single attempt only
def _encrypt(field_name: str, value: str) -> str | None:
    """
    Encrypt a value for the given field name.
    
    Args:
        value: The value to encrypt
        field_name: The name of the field being encrypted
        
    Returns:
        str: The encrypted value
    """
    encrypted_value = None

    skip_encryption = os.getenv("SKIP_ENCRYPTION")

    if skip_encryption:
        logger.info(f"Skipping encryption for field: {field_name}")
        return value

    if not field_name or not value or str(value).lower() in ['nan', 'none', 'null', '']:
        logger.warning(f"Field name or value is None or empty, cannot encrypt")
        return None
    
    logger.info(f"Encrypting value for field: {field_name}")

    function_name = os.getenv("PSEUDONYMISATION_LAMBDA_FUNCTION_NAME")
    if not function_name:
        logger.error("Unable to resolve Pseudonymisation service. PSEUDONYMISATION_LAMBDA_FUNCTION_NAME environment variable is not set")
        raise ValueError("Unable to resolve Pseudonymisation service. PSEUDONYMISATION_LAMBDA_FUNCTION_NAME environment variable is not set")

    try:
        # Encrypt a single value
        lambda_client = boto3.client('lambda')    
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'action': 'encrypt',
                'field_name': field_name,
                'field_value': value
            })
        )

        # check if there is an "error" key in the response
        result = json.loads(response['Payload'].read())
        if 'error' in result:   
            logger.error(f"Encryption service returned error: {result['error']}")
        elif 'field_value' not in result:
            logger.error(f"Encryption service returned malformed response: missing 'field_value' for field '{field_name}'. Response: {result}")
            raise ValueError(f"Encryption service returned malformed response: missing 'field_value' for field '{field_name}'. Response: {result}")
    
        encrypted_value = result['field_value']        
        logger.info(f"Encrypted value for field:{field_name}")

    except ValueError as e:
        # Re-raise ValueError (malformed response) to propagate up the call chain
        raise
    except Exception as e:
        logger.error(f"Exception occurred while encrypting value for field: {field_name}: {str(e)}", exc_info=True)
        raise

    return encrypted_value