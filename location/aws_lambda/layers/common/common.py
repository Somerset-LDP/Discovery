import hashlib
import logging
import os
import tempfile
from datetime import datetime
from io import BytesIO
from typing import Any

import requests

from location.aws_lambda.layers.common.common_utils import DOWNLOAD_TIMEOUT_SECONDS, CHUNK_SIZE_BYTES, DataIngestionException

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))


def download_file(url: str, stream: bool = True) -> BytesIO:
    """
    Download small file from URL to BytesIO buffer.
    Use for small files only (e.g., XLSX). For large files use download_file_to_temp().
    """
    logger.debug(f"Downloading file from {url}")

    if not url:
        raise DataIngestionException("URL is empty or None")

    try:
        response = requests.get(url, stream=stream, timeout=DOWNLOAD_TIMEOUT_SECONDS)

        if response.status_code != 200:
            raise DataIngestionException(
                f"Failed to download file from {url}, HTTP status: {response.status_code}"
            )

        buffer = BytesIO()

        if stream:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    buffer.write(chunk)
        else:
            buffer.write(response.content)

        total_bytes = buffer.getbuffer().nbytes

        if total_bytes == 0:
            raise DataIngestionException(f"Downloaded file from {url} is empty")

        logger.debug(f"Downloaded {total_bytes} bytes from {url}")
        buffer.seek(0)
        return buffer

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


def download_file_to_temp(url: str, suffix: str = '.tmp') -> str:
    logger.debug(f"Downloading file from {url} to temporary file")

    if not url:
        raise DataIngestionException("URL is empty or None")

    temp_file_path = None
    success = False

    try:
        response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS)

        if response.status_code != 200:
            raise DataIngestionException(
                f"Failed to download file from {url}, HTTP status: {response.status_code}"
            )

        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=suffix) as temp_file:
            temp_file_path = temp_file.name
            total_bytes = 0

            logger.debug(f"Downloading to temporary file: {temp_file_path}")

            for chunk in response.iter_content(chunk_size=CHUNK_SIZE_BYTES):
                if chunk:
                    temp_file.write(chunk)
                    total_bytes += len(chunk)
                    # Log progress every 100MB
                    if total_bytes % (100 * 1024 * 1024) == 0:
                        logger.debug(f"Downloaded {total_bytes / (1024 * 1024):.2f} MB")

            if total_bytes == 0:
                raise DataIngestionException(f"Downloaded file from {url} is empty")

            logger.debug(f"Downloaded {total_bytes / (1024 * 1024):.2f} MB total to {temp_file_path}")

        success = True
        return temp_file_path

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
    finally:
        # Clean up temp file only if download failed
        if not success and temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.debug(f"Cleaned up temporary file after error: {temp_file_path}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temporary file {temp_file_path}: {str(cleanup_error)}")


def parse_to_datetime(ingestion_timestamp: str) -> datetime:
    """
    Parse timestamp string to datetime object.
    Accepts various ISO 8601 formats including date-only, datetime, with/without fractional seconds.
    Examples: '2025-12-01', '2025-12-01T10:00:00', '2025-12-01T10:00:00.123456Z'
    """
    if not ingestion_timestamp or not ingestion_timestamp.strip():
        raise DataIngestionException("ingestion_timestamp is empty or None")

    timestamp = ingestion_timestamp.strip().rstrip('Z')
    formats = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(timestamp, fmt)
        except ValueError:
            continue

    raise DataIngestionException(
        f"Invalid ingestion_timestamp format: '{ingestion_timestamp}'. "
        f"Expected ISO 8601 format (e.g., '2025-12-01' or '2025-12-01T10:00:00' or '2025-12-01T10:00:00.123Z')"
    )


def calculate_sha256_checksum(file_name: str, file_stream: Any, chunk_size: int = CHUNK_SIZE_BYTES) -> str:
    try:
        logger.debug(f"Calculating checksum for file: {file_name}")
        sha256_hash = hashlib.sha256()

        while chunk := file_stream.read(chunk_size):
            sha256_hash.update(chunk)
        checksum = sha256_hash.hexdigest()
        logger.debug(f"Checksum for file: {file_name} calculated, checksum: {checksum}")
        return checksum
    except Exception as e:
        raise DataIngestionException(f"Failed to calculate checksum for file: {file_name}, exception: {str(e)}")

