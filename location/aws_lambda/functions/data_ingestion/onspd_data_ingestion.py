import logging
import os
import zipfile
from io import BytesIO
from typing import Union, Optional

from location.aws_lambda.layers.common.common import download_file
from location.aws_lambda.layers.common.common_utils import DataIngestionEvent, DataIngestionException
from location.aws_lambda.layers.common.s3_utils import create_s3_key, upload_to_s3_multipart

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

    logger.debug(f"Downloading ZIP from {url}")
    zip_content = download_file(url, stream=True)

    if not zip_content:
        raise DataIngestionException("Downloaded ZIP file is empty")

    logger.debug(f"Extracting file: {target_file_path}")
    csv_content = extract_file_from_zip(zip_content, target_file_path)

    if not csv_content or csv_content.getbuffer().nbytes == 0:
        raise DataIngestionException(f"Extracted file {target_file_path} is empty")

    file_name = target_file_path.split('/')[-1]
    if not file_name:
        raise DataIngestionException(f"Could not extract filename from path: {target_file_path}")

    s3_key = create_s3_key(ingestion_data.data_source, ingestion_data.ingestion_timestamp, file_name)

    logger.debug(f"Uploading to S3 using multipart upload: {s3_key}")
    upload_to_s3_multipart(ingestion_data.target_bucket, s3_key, csv_content)

    logger.info(f"Successfully ingested {file_name}")


def extract_file_from_zip(zip_content: Union[bytes, BytesIO], target_file_path: Optional[str]) -> BytesIO:
    if not zip_content:
        raise DataIngestionException("Invalid input: zip_content is missing")

    if not target_file_path:
        raise DataIngestionException("Invalid input: target_file_path is missing")

    try:
        with zipfile.ZipFile(zip_content, "r") as zip_file:
            zip_filelist = zip_file.namelist()
            logger.debug(f"ZIP contains {len(zip_filelist)} files")

            if target_file_path not in zip_filelist:
                logger.error(f"File '{target_file_path}' not found. Available files: {zip_filelist}")
                raise DataIngestionException(f"File '{target_file_path}' not found in ZIP archive")

            with zip_file.open(target_file_path) as file_stream:
                content = file_stream.read()
                if not content:
                    raise DataIngestionException(f"File '{target_file_path}' exists but is empty")
                logger.debug(f"Extracted {len(content)} bytes from {target_file_path}")
                return BytesIO(content)

    except zipfile.BadZipFile as e:
        raise DataIngestionException(f"Invalid ZIP file: {str(e)}")
    except zipfile.LargeZipFile as e:
        raise DataIngestionException(f"ZIP file too large: {str(e)}")
    except DataIngestionException:
        raise
    except Exception as e:
        raise DataIngestionException(f"Unexpected error extracting file from ZIP: {str(e)}")
