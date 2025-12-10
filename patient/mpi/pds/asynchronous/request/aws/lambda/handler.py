import logging
import os
from sqlalchemy import create_engine, Engine, text
from typing import Optional, Tuple, Any, Dict
import boto3
from botocore.exceptions import ClientError

from mpi.pds.asynchronous.request.service import PdsAsyncRequestService, SubmitStatus
from mpi.pds.asynchronous.request.trace_status import TraceStatus
from mpi.local.repository import PatientRepository

# runs on a timer to submit batches of outstanding patient trace requests to PDS via MESH

# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")  
logger.setLevel(log_level.upper())

# Global cache
_cached_username = None
_cached_password = None

def lambda_handler(event, context):
    """
    AWS Lambda entrypoint for asynchronous PDS patient trace request submission.
    Wraps mpi.pds.asynchronous.request.service.PdsAsyncRequestService.

    This Lambda is triggered on a schedule to submit batches of outstanding patient trace requests to PDS via MESH.

    Expected event format:
        The event is typically empty or contains metadata for the invocation. No patient data is required in the event.

    Returns a response dict with statusCode and body containing the results.
    The result body includes a message, the AWS request ID, and a status object with:
        - patient_ids: List of patient IDs included in the submission batch.
        - submission_time: ISO8601 timestamp of when the submission occurred.

    Example response:
    {
        "statusCode": 200,
        "body": {
            "message": "PDS Trace submission completed successfully",
            "request_id": "<AWS request ID>",
            "status": {
                "patient_ids": ["patient-id-1", "patient-id-2", ...],
                "submission_time": "2025-12-10T12:34:56.789Z"
            }
        }
    }
    """

    try:    
        logger.info("Starting PDS MESH request submission Lambda execution")
        logger.debug(f"Event: {event}")

        mpi_db_url = _get_mpi_db_url()
        if mpi_db_url:
            engine = _create_db_engine(mpi_db_url)
            if engine is not None:
                trace_status = TraceStatus(engine)
                patient_repository = PatientRepository(engine)  
                service = PdsAsyncRequestService(trace_status, patient_repository)
                status: SubmitStatus = service.submit()

                return {
                    "statusCode": 200,
                    "body": {
                        "message": "PDS Trace submission completed successfully",
                        "request_id": context.aws_request_id,
                        "status": {
                            "patient_ids": status["patient_ids"],
                            "submission_time": status["submission_time"].isoformat() if status["submission_time"] else None
                        }
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
        logger.error(f"PDS Trace submission failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": f"PDS Trace submission failed: {str(e)}",
                "request_id": context.aws_request_id
            }
        }                  

    # TODO - return some status object?

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
