import logging
import os

from location.aws_lambda.layers.common.common import download_file_to_temp
from location.aws_lambda.layers.common.common_utils import (
    DataIngestionEvent,
    DataIngestionException
)
from location.aws_lambda.layers.common.s3_utils import create_s3_key, upload_from_zip_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))


def ingest_onspd_data(ingestion_data: DataIngestionEvent) -> None:
    logger.debug(f"Downloading and ingesting data for {ingestion_data.data_source} started")

    url = os.environ.get("ONSPD_URL")
    target_file_path = os.environ.get("ONSPD_TARGET_PREFIX")

    if not url:
        raise DataIngestionException("ONSPD_URL environment variable is not set")
    if not target_file_path:
        raise DataIngestionException("ONSPD_TARGET_PREFIX environment variable is not set")

    file_name = target_file_path.split('/')[-1]
    if not file_name:
        raise DataIngestionException(f"Could not extract filename from path: {target_file_path}")

    s3_key = create_s3_key(ingestion_data.data_source, ingestion_data.ingestion_timestamp, file_name)

    logger.debug(f"Processing ZIP from {url} and extracting {target_file_path}")

    temp_zip_path = None
    try:
        temp_zip_path = download_file_to_temp(url, suffix='.zip')
        upload_from_zip_to_s3(temp_zip_path, target_file_path, ingestion_data.target_bucket, s3_key)
        logger.info(f"Successfully ingested {file_name}")
    finally:
        if temp_zip_path and os.path.exists(temp_zip_path):
            try:
                os.unlink(temp_zip_path)
                logger.debug(f"Cleaned up temporary file: {temp_zip_path}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_zip_path}: {str(e)}")
