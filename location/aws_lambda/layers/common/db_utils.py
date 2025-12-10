import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from location.aws_lambda.layers.common.common_utils import DataIngestionException, DataIngestionStatus
from location.aws_lambda.layers.common.secrets_manager_utils import get_secret_value

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG"))

INGEST_TABLE_NAME = "ldp_file_ingest_log"
INGEST_SCHEMA_NAME = "location"

cached_username: Optional[str] = None
cached_password: Optional[str] = None


@dataclass
class IngestRecord:
    dataset_key: str
    file_name: str
    checksum: str
    status: str
    ingested_at: datetime
    rows_bronze: Optional[int] = None


def get_db_credentials() -> tuple[str, str]:
    global cached_username, cached_password

    if cached_username and cached_password:
        return cached_username, cached_password

    username_secret = os.environ.get("LDP_DB_USERNAME_SECRET")
    password_secret = os.environ.get("LDP_DB_PASSWORD_SECRET")

    if not username_secret or not password_secret:
        raise DataIngestionException("LDP_DB_USERNAME_SECRET and LDP_DB_PASSWORD_SECRET environment variables are required")

    cached_username = get_secret_value(username_secret)
    cached_password = get_secret_value(password_secret)

    return cached_username, cached_password


def get_connection():
    host = os.environ.get("LDP_DB_HOST")
    port = os.environ.get("LDP_DB_PORT", "5432")
    database = os.environ.get("LDP_DB_NAME", "ldp")

    if not host:
        raise DataIngestionException("LDP_DB_HOST environment variable is required")

    username, password = get_db_credentials()

    logger.debug(f"Connecting to database {host}:{port}/{database}")

    try:
        return psycopg2.connect(
            host=host,
            database=database,
            user=username,
            password=password,
            port=int(port)
        )
    except psycopg2.Error as e:
        raise DataIngestionException(f"Database connection failed: {str(e)}")


def get_ingest_record(dataset_key: str, file_name: str) -> Optional[IngestRecord]:
    if not dataset_key or not file_name:
        raise DataIngestionException("dataset_key and file_name are required")

    logger.debug(f"Fetching ingest record: dataset_key={dataset_key}, file_name={file_name}")

    query = sql.SQL("""
        SELECT dataset_key, file_name, checksum, status, ingested_at, rows_bronze
        FROM {schema}.{table}
        WHERE dataset_key = %s AND file_name = %s
    """).format(
        schema=sql.Identifier(INGEST_SCHEMA_NAME),
        table=sql.Identifier(INGEST_TABLE_NAME)
    )

    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, (dataset_key, file_name))
                row = cursor.fetchone()

                if not row:
                    logger.debug(f"No record found for dataset_key={dataset_key}, file_name={file_name}")
                    return None

                record = IngestRecord(
                    dataset_key=row["dataset_key"],
                    file_name=row["file_name"],
                    checksum=row["checksum"],
                    status=row["status"],
                    ingested_at=row["ingested_at"],
                    rows_bronze=row.get("rows_bronze")
                )
                logger.debug(f"Found record: {record}")
                return record

    except psycopg2.Error as e:
        raise DataIngestionException(f"Failed to fetch ingest record: {str(e)}")


def upsert_ingest_record(
    dataset_key: str,
    file_name: str,
    checksum: str,
    status: DataIngestionStatus,
    ingested_at: datetime
) -> None:
    if not dataset_key or not file_name or not checksum:
        raise DataIngestionException("dataset_key, file_name, and checksum are required")

    logger.debug(f"Upserting ingest record: dataset_key={dataset_key}, file_name={file_name}, status={status.value}")

    query = sql.SQL("""
        INSERT INTO {schema}.{table} (dataset_key, file_name, checksum, status, ingested_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (dataset_key, file_name)
        DO UPDATE SET
            checksum = EXCLUDED.checksum,
            status = EXCLUDED.status,
            ingested_at = EXCLUDED.ingested_at
    """).format(
        schema=sql.Identifier(INGEST_SCHEMA_NAME),
        table=sql.Identifier(INGEST_TABLE_NAME)
    )

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (dataset_key, file_name, checksum, status.value, ingested_at))
            conn.commit()
            logger.info(f"Upserted ingest record: dataset_key={dataset_key}, file_name={file_name}, status={status.value}")

    except psycopg2.Error as e:
        raise DataIngestionException(f"Failed to upsert ingest record: {str(e)}")
