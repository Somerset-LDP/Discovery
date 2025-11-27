import json
import logging
import os
from typing import Any, Dict, Tuple
import pandas as pd
import fsspec
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, Engine
from canonical_processor import run
from canonical_feed_config import get_feed_config, FEED_CONFIGS


# Configure logging
logger = logging.getLogger()
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level.upper())

# Global cache
_cached_username = None
_cached_password = None


def _validate_event(event: Dict[str, Any]) -> str | None:
    if not event:
        return "Event is empty"
    
    if 'feed_type' not in event:
        return "Missing required parameter: feed_type"
    
    if 'input_path' not in event:
        return "Missing required parameter: input_path"
    
    feed_type = event['feed_type']
    if not isinstance(feed_type, str) or not feed_type.strip():
        return "Invalid feed_type: must be a non-empty string"
    
    input_path = event['input_path']
    if not isinstance(input_path, str) or not input_path.strip():
        return "Invalid input_path: must be a non-empty string"

    if feed_type.lower() not in FEED_CONFIGS:
        supported_feeds = ', '.join(FEED_CONFIGS.keys())
        return f"Unsupported feed_type: {feed_type}. Supported types: {supported_feeds}"
    
    return None


def lambda_handler(event, context):
    # read file from S3 -  df = pd.read_csv("path/to/file.csv", header=[0, 1])
    # pass to run function from layers/canonical/pipeline/canonical_processor.py
    # write output to our database
    response = {}

    try:
        logger.info("Starting canonical pipeline execution")
        logger.info(f"Event: {json.dumps(event, default=str)}")

        validation_error = _validate_event(event)
        if validation_error:
            return _get_response(
                message=validation_error,
                request_id=context.aws_request_id,
                status_code=400
            )

        feed_type = event['feed_type'].lower()
        input_path = event['input_path']
        output_location = _get_output_db_url()

        if not output_location:
            return _get_response(
                message='Failed to configure database connection',
                request_id=context.aws_request_id,
                status_code=500
            )

        logger.info(f"Processing feed type: {feed_type.upper()}")
        logger.info(f"Input path: {input_path}")
        
        input = _read_patients(input_path, feed_type)
        output = run(input, feed_type)
        _write_patients(output, create_engine(output_location))

        logger.info(f"{feed_type.upper()} pipeline execution completed successfully")
        response = _get_response(
            message=f"{feed_type.upper()} pipeline execution completed successfully",
            request_id=context.aws_request_id,
            status_code=200,
            records_processed=len(input),
            records_stored=len(output),
            feed_type=feed_type
        )
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}", exc_info=True)
        response = _get_response(
            message=f"Validation error: {str(e)}",
            request_id=context.aws_request_id,
            status_code=400
        )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        
        response = _get_response(
            message=f"Pipeline execution failed: {str(e)}",
            request_id= context.aws_request_id,
            status_code=500
        )

    return response


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

def _read_patients(path: str, feed_type: str) -> pd.DataFrame:
    feed_config = get_feed_config(feed_type)

    try:
        with fsspec.open(path, mode="r", encoding="utf-8") as file:
            if isinstance(file, list):
                raise ValueError(f"Expected one file, got {len(file)}: {path}")

            # Read CSV with all columns as strings to preserve leading zeros and handle data consistently
            df = pd.read_csv(
                file,
                dtype=str,
                keep_default_na=False,
                na_values=['', 'NULL', 'null', 'None'],
                skiprows=feed_config.metadata_rows_to_skip,
                header=0  # Single header row
            )

            logger.info(f"Successfully read {len(df)} records from {path}")
            return df

    except FileNotFoundError as e:
        logger.error(f"File not found: {path}")
        raise ValueError(f"File not found: {path}") from e
    except PermissionError as e:
        logger.error(f"Permission denied reading file: {path}")
        raise RuntimeError(f"Permission denied reading file: {path}") from e
    except pd.errors.EmptyDataError as e:
        logger.error(f"CSV file is empty: {path}")
        raise ValueError(f"CSV file is empty: {path}") from e
    except pd.errors.ParserError as e:
        logger.error(f"Failed to parse CSV file: {path} - {str(e)}")
        raise ValueError(f"Failed to parse CSV file: {path}") from e
    except Exception as e:
        logger.error(f"Unexpected error reading file {path}: {str(e)}", exc_info=True)
        raise RuntimeError(f"Failed to read file: {str(e)}") from e

def _write_patients(output_df: pd.DataFrame, engine: Engine):    
    """
    Store the canonical Patient records DataFrame to a relational database table.
    
    Args:
        output_df: DataFrame containing canonical Patient records
        engine: SQLAlchemy engine for database connection
    """
    schema = os.environ.get("OUTPUT_DB_SCHEMA", "canonical")
    table = os.environ.get("OUTPUT_DB_TABLE", "patient")

    try:
        output_df.to_sql(table, engine, if_exists="append", index=False, schema=schema)
        logger.info(f"Successfully wrote {len(output_df)} canonical Patient records to {schema}.{table}")
    except Exception as e:
        logger.error(f"Failed to write canonical Patient records to database: {e}", exc_info=True)
        raise RuntimeError(f"Failed to write canonical Patient records to database: {str(e)}") 
    
def _get_secret_value(secret_name: str) -> str | None:
    """
    Fetches a raw (non-JSON) secret string from AWS Secrets Manager.
    """
    secret_string = None

    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve secret '{secret_name}': {e}") from e

    secret_string = response.get("SecretString")
    if not secret_string:
        logger.error(f"Secret '{secret_name}' has no SecretString value.")

    return secret_string

def _get_db_credentials() -> Tuple[str | None, str | None]:
    """
    Returns a tuple (username, password) using cached values if available.
    """
    # For testing - check direct env vars first
    username = os.environ.get("OUTPUT_DB_USERNAME")
    password = os.environ.get("OUTPUT_DB_PASSWORD")

    if username and password:
        return username, password

    global _cached_username, _cached_password

    OUTPUT_DB_USERNAME_SECRET = os.environ.get("OUTPUT_DB_USERNAME_SECRET")
    OUTPUT_DB_PASSWORD_SECRET = os.environ.get("OUTPUT_DB_PASSWORD_SECRET")

    if not OUTPUT_DB_USERNAME_SECRET or not OUTPUT_DB_PASSWORD_SECRET:
        logger.error(f"Missing environment variables for database credentials")
        return None, None

    # Only fetch secrets that are not yet cached
    global _cached_username, _cached_password

    if _cached_username is None:
        _cached_username = _get_secret_value(OUTPUT_DB_USERNAME_SECRET)

    if _cached_password is None:
        _cached_password = _get_secret_value(OUTPUT_DB_PASSWORD_SECRET)

    return (_cached_username, _cached_password)

def _get_output_db_url() -> str | None:
    """
    Constructs the database URL using credentials and environment variables.
    """
    DB_HOST = os.environ.get("OUTPUT_DB_HOST")
    DB_PORT = os.environ.get("OUTPUT_DB_PORT", "5432")
    DB_NAME = os.environ.get("OUTPUT_DB_NAME", "ldp")

    username, password = _get_db_credentials()

    if not DB_HOST or not DB_NAME or not username or not password:
        logger.error(f"Missing environment variables or credentials for database URL")
        return None

    return f"postgresql+psycopg2://{username}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"