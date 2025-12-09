"""
AWS Lambda entrypoint for synchronous Patient Matching requests.
Wraps matching.service.MatchingService.
"""
import logging
import os
from typing import Optional, Tuple, Any, Dict
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from sqlalchemy import create_engine, Engine, text
from matching.service import MatchingService
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
    AWS Lambda handler for patient matching requests.
    
    Expected event format (note that all values are optional but the more values that are provided,
    the better the matching accuracy):
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

    Returns a response dict with statusCode and body containing the results. 
    The result body includes the original patient data with an added 'patient_ids' field listing 
    matched patient IDs. This could be multiple patient IDs if the patient data matches more 
    than one record. If no matches are found, 'patient_ids' will be an empty list:
    {
        "statusCode": 200,
        "body": {
            "message": "Patient Matching completed successfully",
            "request_id": "<AWS request ID>",
            "counts": {
                "total": <total number of records processed>,
                "single": <number of records with a single match>,
                "multiple": <number of records with multiple matches>,
                "zero": <number of records with no matches>
            },
            "data": [
                {
                    "nhs_number": "1234567890",
                    "first_name": "John",
                    "last_name": "Doe",
                    "postcode": "SW1A 1AA",
                    "dob": "1980-01-15",
                    "sex": "male",
                    "patient_ids": ["patient-id-1", "patient-id-2"]
                },
                ...
            ]
        }
    }
    """

    try:
        logger.info("Starting Patient Matching Lambda execution")
        logger.debug(f"Event: {event}")

        df = _to_dataframe(event)

        logger.info(f"Processing {len(df)} input patient records")

        mpi_db_url = _get_mpi_db_url()
        if mpi_db_url:
            engine = _create_db_engine(mpi_db_url)
            if engine is not None:
                service = MatchingService(local_mpi=PatientRepository(engine))
                matched_df = service.match(df)

                # Convert result DataFrame to dict for response
                result = matched_df.to_dict(orient='records')

                logger.info("Patient Matching Lambda execution completed successfully")

                return {
                    "statusCode": 200,
                    "body": {
                        "message": "Patient Matching completed successfully",
                        "request_id": context.aws_request_id,
                        "counts": __count_matches(matched_df),
                        "data": result
                    }
                }
            else:
                error_message = 'Failed to create database engine'
                logger.error(error_message)
                return {
                    "statusCode": 500,
                    "body": {
                        "message": error_message,
                        "request_id": context.aws_request_id
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
        logger.error(f"Patient Matching Lambda execution failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": f"Patient Matching execution failed: {str(e)}",
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
    DB_SCHEMA = os.environ.get("MPI_SCHEMA_NAME", "mpi")

    username, password = _get_db_credentials()

    if not DB_HOST or not DB_NAME or not username or not password:
        logger.error(f"Missing environment variables or credentials for database URL")
        return None

    return f"postgresql+psycopg2://{username}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-c%20search_path={DB_SCHEMA},public"

def __count_matches(all_matches: pd.DataFrame) -> Dict[str, int]:
        """Implements patient matching using SQL exact matching.

        Args:
            queries: DataFrame with patient data to match against local MPI.
        """
        # Calculate match statistics
        match_stats = {
            'total': len(all_matches),
            'single': 0,
            'multiple': 0,
            'zero': 0
        }

        for patient_ids in all_matches['patient_ids']:
            if len(patient_ids) == 0:
                match_stats['zero'] += 1
            elif len(patient_ids) == 1:
                match_stats['single'] += 1
            else:
                match_stats['multiple'] += 1            

        return match_stats

def _create_db_engine(mpi_db_url: str) -> Optional[Engine]:
    """Creates a SQLAlchemy engine for the MPI database."""
    engine: Optional[Engine] = None
    
    if mpi_db_url:
        try:
            engine = create_engine(mpi_db_url)
            logger.info("Created engine for MPI database")
            # Force connection to catch errors early
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.close()
                logger.info("Database connection successful")
        except Exception as db_exc:
            logger.error(f"Database connection failed: {str(db_exc)}", exc_info=True)
            engine = None
        
    return engine