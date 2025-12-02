import logging
import os

from location.aws_lambda.layers.common.common import download_file
from location.aws_lambda.layers.common.common_utils import DataIngestionEvent, DataIngestionException
from location.aws_lambda.layers.common.s3_utils import create_s3_key, upload_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))


def ingest_imd_data(ingestion_data: DataIngestionEvent) -> None:
    logger.debug(f"Downloading and ingesting data for {ingestion_data.data_source} started")
    url = os.environ.get("IMD_URL")
    file_name = os.environ.get("IMD_TARGET_PREFIX")

    if not url:
        raise DataIngestionException("IMD_URL environment variable is not set")
    if not file_name:
        raise DataIngestionException("IMD_TARGET_PREFIX environment variable is not set")

    logger.debug(f"Downloading XLSX from {url}")
    xlsx_content = download_file(url, stream=False)

    if not xlsx_content or xlsx_content.getbuffer().nbytes == 0:
        raise DataIngestionException("Downloaded XLSX file is empty")

    logger.debug(f"Downloaded {xlsx_content.getbuffer().nbytes} bytes")
    s3_key = create_s3_key(ingestion_data.data_source, ingestion_data.ingestion_timestamp, file_name)
    logger.debug(f"Uploading to S3: {s3_key}")
    upload_to_s3(ingestion_data.target_bucket, s3_key, xlsx_content)
    logger.info(f"Successfully ingested {file_name}")
