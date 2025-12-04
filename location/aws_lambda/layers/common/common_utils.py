from dataclasses import dataclass
from enum import Enum

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
