import json
import logging
import os
from typing import Any, Dict
import pandas as pd
import fsspec
from sqlalchemy import create_engine, Engine
from pipeline.emis_gprecord import run

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # read file from S3 -  df = pd.read_csv("path/to/file.csv", header=[0, 1])
    # pass to run function from layers/canonical/pipeline/emis_gprecord.py
    # write output to our database
    response = {}

    try:
        logger.info("Starting GP pipeline execution")
        logger.info(f"Event: {json.dumps(event, default=str)}")

        input_location = os.getenv("INPUT_LOCATION")
        output_location = os.getenv("OUTPUT_LOCATION")   

        if input_location and output_location:
            input = _read_patients(input_location)
            output = run(input)
            _write_patients(output, create_engine(output_location))

            # log success return success response
            logger.info("GP pipeline execution completed successfully")
            response = _get_response(
                message="GP pipeline execution completed successfully",
                request_id=context.aws_request_id,
                status_code=200,
                records_processed=len(input),
                records_stored=len(output)
            )
        else:
            response = _get_response(
                message='Missing one or more of the required environment variables - INPUT_LOCATION, OUTPUT_LOCATION',
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

def _read_patients(path: str) -> pd.DataFrame:
    with fsspec.open(path, mode="r", encoding="utf-8") as file:
        if isinstance(file, list):
            raise ValueError(f"Expected one file, got {len(file)}: {path}")

        # Read CSV with all columns as strings to preserve leading zeros and handle data consistently
        df = pd.read_csv(file, dtype=str, keep_default_na=False, na_values=['', 'NULL', 'null', 'None'],  skiprows=10, header=[0, 1])

    return df    

def _write_patients(output_df: pd.DataFrame, engine: Engine):    
    """
    Store the canonical Patient records DataFrame to a relational database table.
    
    Args:
        output_df: DataFrame containing canonical Patient records
        engine: SQLAlchemy engine for database connection
    """
    try:
        output_df.to_sql("patient", engine, if_exists="append", index=False, schema="canonical")
        logger.info(f"Successfully wrote {len(output_df)} canonical Patient records to database")
    except Exception as e:
        logger.error(f"Failed to write canonical Patient records to database: {e}", exc_info=True)
        raise RuntimeError(f"Failed to write canonical Patient records to database: {str(e)}") 