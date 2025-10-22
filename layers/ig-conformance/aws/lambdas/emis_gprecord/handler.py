import json
import logging
import os
import pandas as pd
from typing import Any, List, Dict, cast
from pipeline.emis_gprecord import run
from common.cohort_membership import read_cohort_members

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
            gp_records = _read_gp_records(gp_records_store_location)
            cohort_store = read_cohort_members(cohort_store_location)

            cohort_member_records = run(cohort_store, gp_records)

            # Write the filtered results to output file
            if cohort_member_records:
                _write_gp_records(cohort_member_records, output_location)
                logger.info(f"Wrote {len(cohort_member_records)} records to {output_location}")

            # TODO - delete the gp records as the LDP is not permitted to store raw data that holds plain text PII

            logger.info("GP pipeline executed successfully")
            logger.debug(f"Processed {len(gp_records)} records, filtered to {len(cohort_member_records)} records.")
            
            response = _get_response(
                message='GP pipeline executed successfully',
                request_id=context.aws_request_id,
                status_code=200,
                records_processed=len(gp_records),
                records_retained=len(cohort_member_records)               
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

def _read_gp_records(location: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(location, dtype={'nhs': str})

    # Ensure all column names are strings
    df.columns = df.columns.astype(str)    

    return cast(List[Dict[str, Any]], df.to_dict(orient='records'))


def _write_gp_records(records: List[Dict[str, Any]], location: str) -> None:
    """
    Write GP records to CSV file in the same format as input
    
    Args:
        records: List of record dictionaries to write
        location: Output file location (local path or S3 URL)
    """
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
    df.to_csv(location, index=False)

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