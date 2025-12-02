import logging
import os
from datetime import datetime
from io import BytesIO

import requests

from location.aws_lambda.layers.common.common_utils import (
    EXPECTED_CONTENT_TYPES,
    DOWNLOAD_TIMEOUT_SECONDS,
    CHUNK_SIZE_BYTES,
    ISO_8601_TIMESTAMP_FORMAT_MICROSECONDS,
    ISO_8601_TIMESTAMP_FORMAT_NO_FRACTION,
    DataIngestionException
)

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))


def download_file(url: str, stream: bool = True) -> BytesIO:
    logger.debug(f"Downloading file from {url} started")

    if not url:
        raise DataIngestionException("URL is empty or None")

    try:
        response = requests.get(url, stream=stream, timeout=DOWNLOAD_TIMEOUT_SECONDS)

        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            logger.debug(f"Content-Type: {content_type}")

            content_type_lower = content_type.lower().split(';')[0].strip()

            if content_type_lower in EXPECTED_CONTENT_TYPES:
                buffer = BytesIO()
                total_bytes = 0

                for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                    if chunk:
                        buffer.write(chunk)
                        total_bytes += len(chunk)

                if total_bytes == 0:
                    raise DataIngestionException(f"Downloaded file from {url} is empty")

                logger.debug(f"Downloaded {total_bytes} bytes from {url} in streaming mode")
                buffer.seek(0)
                return buffer
            else:
                logger.error(f"Unexpected content type: {content_type}. Expected one of: {EXPECTED_CONTENT_TYPES}")
                raise DataIngestionException(f"Unsupported content type: {content_type}")
        else:
            raise DataIngestionException(
                f"Failed to download file from {url}, HTTP status: {response.status_code}"
            )

    except requests.exceptions.Timeout:
        raise DataIngestionException(f"Timeout downloading file from {url} (timeout: {DOWNLOAD_TIMEOUT_SECONDS}s)")
    except requests.exceptions.ConnectionError as e:
        raise DataIngestionException(f"Connection error downloading file from {url}: {str(e)}")
    except requests.exceptions.RequestException as e:
        raise DataIngestionException(f"Request error downloading file from {url}: {str(e)}")
    except DataIngestionException:
        raise
    except Exception as e:
        raise DataIngestionException(f"Unexpected error downloading file from {url}: {str(e)}")


def is_date_format_valid(date: str, date_format: str) -> bool:
    try:
        datetime.strptime(date, date_format)
        return True
    except ValueError:
        return False


def parse_to_datetime(ingestion_timestamp: str) -> datetime:
    timestamp = ingestion_timestamp.rstrip('Z')

    try:
        return datetime.strptime(timestamp, ISO_8601_TIMESTAMP_FORMAT_MICROSECONDS)
    except ValueError:
        pass

    try:
        return datetime.strptime(timestamp, ISO_8601_TIMESTAMP_FORMAT_NO_FRACTION)
    except ValueError:
        raise DataIngestionException(
            f"Invalid ingestion_timestamp format: {ingestion_timestamp}, expected format is 'YYYY-MM-DDTHH:MM:SS.fff' or 'YYYY-MM-DDTHH:MM:SS.ffffff'"
        )
