from datetime import datetime
import json
import logging
import os
from sys import path
import fsspec
import fsspec.utils
import pandas as pd
from typing import Any, List, Dict, TextIO, cast, Tuple
from pipeline.emis_gprecord import run
from common.cohort_membership import read_cohort_members
from common.filesystem import read_file, delete_file

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        if cohort_store_location and gp_records_store_location and output_location:
            cohort_member_records, gp_records = _retain_cohort_members(cohort_store_location, gp_records_store_location)
            output_file = _write_output(cohort_member_records, output_location)
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
                message='Missing one or more of the required environment variables - COHORT_STORE, INPUT_LOCATION, OUTPUT_LOCATION',
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

def _retain_cohort_members(cohort_store_location, gp_records_store_location) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Main function to retain GP records for cohort members.
    Reads environment variables for input/output locations.
    """
    cohort_member_records = []
    gp_records = []

    if cohort_store_location and gp_records_store_location:
        gp_records = _read_gp_records(gp_records_store_location)
        cohort_store = read_cohort_members(cohort_store_location)

        cohort_member_records = run(cohort_store, gp_records)
        logger.debug(f"Processed {len(gp_records)} records, filtered to {len(cohort_member_records)} records.")

    return (cohort_member_records, gp_records)

# File system methods

def _read_gp_records(location: str) -> List[Dict[str, Any]]:
    df = read_file(location)
    #df = pd.read_csv(location, dtype={'nhs': str})

    # Ensure all column names are strings
    df.columns = df.columns.astype(str)    

    return cast(List[Dict[str, Any]], df.to_dict(orient='records'))

def _write_gp_records(records: List[Dict[str, Any]], location: str) -> str:
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

    if not records:
        # Create empty DataFrame with no columns if no records
        df = pd.DataFrame()
    else:
        # Convert list of dictionaries back to DataFrame
        df = pd.DataFrame(records)
        
        # Ensure nhs column is treated as string to preserve formatting
        if 'nhs_number' in df.columns:
            df['nhs_number'] = df['nhs_number'].astype(str)
    
    # Write to CSV with same format as input
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Successfully wrote {len(records)} records to {file_path}")
    except Exception as e:
        logger.error(f"Failed to write GP records to {file_path}: {str(e)}", exc_info=True)
        raise IOError(f"Failed to write GP records to {file_path}: {str(e)}")

    return file_path

def _write_output(cohort_member_records, output_location) -> str | None:
    # Write the filtered results to output file
    output_file = None
    if cohort_member_records:
        output_file = _write_gp_records(cohort_member_records, output_location)
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