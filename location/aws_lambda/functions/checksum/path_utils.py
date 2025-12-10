import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG"))


@dataclass
class S3PathInfo:
    dataset_key: str
    file_name: str
    full_key: str
    bronze_key: str


def parse_landing_path(object_key: str) -> Optional[S3PathInfo]:
    """
    Expected format: landing/reference/{dataset}/{date_folder}/.../{file_name}

    Examples:
        - landing/reference/onspd/2024/02/05/ONSPD_FEB_2024_UK.csv
          → dataset_key='reference/onspd', file_name='ONSPD_FEB_2024_UK.csv'
        - landing/reference/imd2019/2025/12/08/IMD2019_English_LSOA.csv
          → dataset_key='reference/imd2019', file_name='IMD2019_English_LSOA.csv'
    """
    if not object_key:
        logger.warning("Empty object key provided")
        return None

    parts = object_key.split("/")

    if len(parts) < 4:
        logger.warning(f"Path too short to parse: {object_key}")
        return None

    if parts[0] != "landing" or parts[1] != "reference":
        logger.warning(f"Path does not start with 'landing/reference/': {object_key}")
        return None

    if object_key.endswith("/"):
        logger.debug(f"Skipping directory marker: {object_key}")
        return None

    dataset_name = parts[2]
    file_name = parts[-1]
    dataset_key = f"reference/{dataset_name}"
    bronze_key = "bronze/" + "/".join(parts[1:])

    path_info = S3PathInfo(
        dataset_key=dataset_key,
        file_name=file_name,
        full_key=object_key,
        bronze_key=bronze_key
    )

    logger.debug(f"Parsed path: {path_info}")
    return path_info
