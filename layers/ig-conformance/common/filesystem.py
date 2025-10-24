import fsspec
import fsspec.utils
import pandas as pd
from typing import TextIO
import logging

def read_file(path: str) -> pd.DataFrame:
    with fsspec.open(path, mode="r", encoding="utf-8") as file:
        if isinstance(file, list):
            raise ValueError(f"Expected one file, got {len(file)}: {path}")

        # Read CSV with all columns as strings to preserve leading zeros and handle data consistently
        df = pd.read_csv(file, dtype=str, keep_default_na=False, na_values=['', 'NULL', 'null', 'None'])

    return df

def delete_file(file_path: str):
    """
    Deletes a file from local or remote storage using fsspec.

    Args:
        path (str): Full file path (e.g. file://path, s3://bucket/key, az://container/blob)
    """
    logger = logging.getLogger(__name__)

    protocol = fsspec.utils.get_protocol(file_path)
    fs = fsspec.filesystem(protocol)

    try:
        if fs.exists(file_path):
            fs.rm(file_path)
            logger.info(f"Successfully deleted file {file_path}")
        else:
            logger.warning(f"File not found: {file_path}")
    except (Exception) as e:
        logger.error(f"Failed to delete file {file_path}: {e}")
        raise IOError(f"Failed to delete file {file_path}: {e}")