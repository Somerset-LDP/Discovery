from dataclasses import dataclass
from enum import Enum

EXPECTED_CONTENT_TYPES = [
    "application/zip",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/x-zip-compressed",
    "application/octet-stream"
]

ISO_8601_TIMESTAMP_FORMAT_MICROSECONDS = "%Y-%m-%dT%H:%M:%S.%f"
ISO_8601_TIMESTAMP_FORMAT_NO_FRACTION = "%Y-%m-%dT%H:%M:%S"

DOWNLOAD_TIMEOUT_SECONDS = 60
CHUNK_SIZE_BYTES = 32 * 1024 * 1024  # 32 MB


@dataclass
class DataIngestionException(Exception):
    message: str
    error_code: int = 500


@dataclass
class DataIngestionEvent:
    data_source: str
    target_bucket: str
    ingestion_timestamp: str


class DataIngestionSource(str, Enum):
    ONSPD = "onspd"
    IMD_2019 = "imd_2019"
