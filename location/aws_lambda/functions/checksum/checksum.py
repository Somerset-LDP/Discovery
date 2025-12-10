import logging
import os
from typing import Any

from path_utils import parse_landing_path
from location.aws_lambda.layers.common.common import calculate_sha256_checksum, parse_to_datetime
from location.aws_lambda.layers.common.common_utils import DataIngestionException, DataIngestionStatus
from location.aws_lambda.layers.common.db_utils import get_ingest_record, upsert_ingest_record
from location.aws_lambda.layers.common.s3_utils import (
    copy_s3_object,
    delete_s3_object,
    get_s3_object_stream,
    parse_s3_event
)

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG"))

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")


def handler(event: dict, context: Any) -> dict:
    logger.debug(f"Received event: {event}")

    if not BRONZE_BUCKET:
        raise DataIngestionException("BRONZE_BUCKET environment variable is not set")

    try:
        s3_event = parse_s3_event(event)
        logger.debug(f"Processing file: s3://{s3_event.bucket}/{s3_event.key}")

        path_info = parse_landing_path(s3_event.key)
        if not path_info:
            logger.info(f"Skipping non-reference path: {s3_event.key}")
            return {"status": "skipped", "reason": "non-reference path"}

        stream = get_s3_object_stream(s3_event.bucket, s3_event.key)
        checksum = calculate_sha256_checksum(path_info.file_name, stream)

        existing_record = get_ingest_record(path_info.dataset_key, path_info.file_name)

        if is_duplicate(existing_record, checksum):
            logger.info(f"Duplicate file detected, removing from Landing: {path_info.file_name}")
            delete_s3_object(s3_event.bucket, s3_event.key)
            return {"status": "skipped", "reason": "duplicate", "checksum": checksum}

        copy_s3_object(s3_event.bucket, path_info.full_key, BRONZE_BUCKET, path_info.bronze_key)

        ingested_at = parse_to_datetime(s3_event.ingestion_timestamp)
        upsert_ingest_record(
            dataset_key=path_info.dataset_key,
            file_name=path_info.file_name,
            checksum=checksum,
            status=DataIngestionStatus.BRONZE_DONE,
            ingested_at=ingested_at
        )

        delete_s3_object(s3_event.bucket, s3_event.key)

        logger.info(f"Successfully processed {path_info.file_name} to Bronze")
        return {
            "status": "success",
            "dataset_key": path_info.dataset_key,
            "file_name": path_info.file_name,
            "checksum": checksum,
            "bronze_key": path_info.bronze_key
        }

    except DataIngestionException as e:
        logger.error(f"Processing error: {e.message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise DataIngestionException(f"Unexpected error: {str(e)}")


def is_duplicate(existing_record, checksum: str) -> bool:
    if not existing_record:
        return False

    if existing_record.checksum != checksum:
        return False

    done_statuses = {DataIngestionStatus.BRONZE_DONE.value, DataIngestionStatus.SILVER_DONE.value}
    return existing_record.status in done_statuses
