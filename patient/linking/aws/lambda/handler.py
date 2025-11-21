"""
AWS Lambda entrypoint for synchronous Patient Linking requests.
Wraps linking.service.LinkageService.
"""
import logging
import os
from typing import Tuple, Any
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from sqlalchemy import create_engine, Engine
from linking.service import LinkageService
from mpi.local.repository import PatientRepository

# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")  
logger.setLevel(log_level.upper())

# Global cache
_cached_username = None
_cached_password = None

def lambda_handler(event, context):
    """
    AWS Lambda handler for patient linking requests.
    
    Expected event format:
    {
        "patients": [
            {
                "nhs_number": "1234567890",
                "first_name": "John",
                "last_name": "Doe",
                "postcode": "SW1A 1AA",
                "dob": "1980-01-15",
                "sex": "male"
            },
            ...
        ]
    }
    """

    try:
        logger.info("Starting Patient Linking Lambda execution")
        logger.info(f"Event: {event}")

        df = _to_dataframe(event)

        mpi_db_url = _get_mpi_db_url()
        if mpi_db_url:
            service = LinkageService(local_mpi=PatientRepository(create_engine(mpi_db_url)))
            linked_df = service.link(df)

            # Convert result DataFrame to dict for response
            result = linked_df.to_dict(orient='records')

            logger.info("Patient Linking Lambda execution completed successfully")

            return {
                "statusCode": 200,
                "body": {
                    "message": "Patient Linking completed successfully",
                    "request_id": context.aws_request_id,
                    "records_processed": len(df),
                    "records_linked": len(linked_df),
                    "data": result
                }
            }
        else:
            error_message = 'Missing MPI database URL configuration'
            logger.error(error_message)
            return {
                "statusCode": 400,
                "body": {
                    "message": error_message,
                    "request_id": context.aws_request_id
                }
            }
    except Exception as e:
        logger.error(f"Patient Linking Lambda execution failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": f"Patient Linking execution failed: {str(e)}",
                "request_id": context.aws_request_id
            }
        }

def _to_dataframe(event: Any) -> pd.DataFrame:
    """
    Converts the event dict to a pandas DataFrame.
    Expects event to have a 'patients' key with a list of patient dicts.
    """
    if not isinstance(event, dict) or 'patients' not in event:
        raise ValueError("Event must be a dict with a 'patients' key")
        
    patients = event.get("patients", [])

    if not isinstance(patients, list):
        raise ValueError("Event 'patients' key must be a list")

    return pd.DataFrame(patients)

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
    username = os.environ.get("MPI_DB_USERNAME")
    password = os.environ.get("MPI_DB_PASSWORD")

    if username and password:
        return username, password

    global _cached_username, _cached_password

    MPI_DB_USERNAME_SECRET = os.environ.get("MPI_DB_USERNAME_SECRET")
    MPI_DB_PASSWORD_SECRET = os.environ.get("MPI_DB_PASSWORD_SECRET")  

    if not MPI_DB_USERNAME_SECRET or not MPI_DB_PASSWORD_SECRET:
        logger.error(f"Missing environment variables for database credentials")
        return None, None

    # Only fetch secrets that are not yet cached
    global _cached_username, _cached_password

    if _cached_username is None:
        _cached_username = _get_secret_value(MPI_DB_USERNAME_SECRET)

    if _cached_password is None:
        _cached_password = _get_secret_value(MPI_DB_PASSWORD_SECRET)

    return (_cached_username, _cached_password)

def _get_mpi_db_url() -> str | None:
    """
    Constructs the database URL using credentials and environment variables.
    """
    DB_HOST = os.environ.get("MPI_DB_HOST")
    DB_PORT = os.environ.get("MPI_DB_PORT", "5432")
    DB_NAME = os.environ.get("MPI_DB_NAME", "ldp")

    username, password = _get_db_credentials()

    if not DB_HOST or not DB_NAME or not username or not password:
        logger.error(f"Missing environment variables or credentials for database URL")
        return None

    return f"postgresql+psycopg2://{username}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"