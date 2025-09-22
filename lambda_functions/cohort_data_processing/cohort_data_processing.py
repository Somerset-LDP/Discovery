import os
import logging
import hashlib
import pandas as pd
from io import StringIO
from typing import List, Set, Tuple, Union

from lambda_functions.cohort_data_processing.s3_utils import write_to_s3, delete_s3_objects, list_s3_files, \
    get_s3_object_content

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_checksum(content: bytes, checksum_content: bytes, key: str) -> None:
    try:
        actual_checksum = hashlib.sha256(content).hexdigest()
        expected_checksum = checksum_content.decode('utf-8').strip()
        if actual_checksum != expected_checksum:
            logger.error(f'Checksum mismatch for {key}: expected {expected_checksum}, got {actual_checksum}')
            raise ValueError(f'Checksum mismatch for {key}')
    except Exception as e:
        logger.error(f'Error in checksum validation for {key}: {e}')
        raise


def write_cohort(gp_bucket: str, cohort_key: str, all_common: Set[str]) -> None:
    try:
        write_to_s3(gp_bucket, cohort_key, all_common)
    except Exception as e:
        logger.error(f'Error writing cohort file: {e}')
        raise


def delete_and_log_remaining(bucket: str, keys: List[str], prefix: str = None) -> None:
    try:
        delete_s3_objects(bucket, keys)
        if prefix is None and keys:
            prefix = os.path.dirname(keys[0])
        remaining = list_s3_files(bucket, prefix) if prefix else []
        if remaining:
            logger.warning(f'Not all files deleted in s3://{bucket}/{prefix}. Remaining: {remaining} (count: {len(remaining)})')
        else:
            logger.info(f'All source files deleted in s3://{bucket}/{prefix}')
    except Exception as e:
        logger.error(f'Error deleting files: {e}')
        raise


def is_valid_nhs_number(nhs_number: Union[str, int, None]) -> bool:
    nhs_number = str(nhs_number).replace(' ', '').strip()
    if not nhs_number.isdigit() or len(nhs_number) != 10:
        return False
    digits = [int(d) for d in nhs_number]
    # NHS checksum: (sum of (digit * (10 - position))) % 11 == 11 - check_digit (or 0 if result is 11)
    total = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
    remainder = total % 11
    check_digit = 11 - remainder
    if check_digit == 11:
        check_digit = 0
    if check_digit == 10:
        return False
    return check_digit == digits[9]


def clean_and_validate_nhs_df(df: pd.DataFrame, nhs_col: str = 'nhs') -> pd.DataFrame:
    df[nhs_col] = df[nhs_col].astype(str).str.replace(' ', '').str.strip()
    df = df[df[nhs_col] != '']
    valid_mask = df[nhs_col].apply(is_valid_nhs_number)
    return df[valid_mask]


def load_and_clean_nhs_csv(
    bucket: str, key: str, checksum_bucket: str, checksum_key: str, filetype: str
) -> Tuple[pd.DataFrame, str]:
    filename = key.split('/')[-1]
    content = get_s3_object_content(bucket, key)
    checksum_content = get_s3_object_content(checksum_bucket, checksum_key)
    validate_checksum(content, checksum_content, key)
    if not content.strip():
        logger.error(f'{filetype} file s3://{bucket}/{key} is empty. Aborting lambda execution.')
        raise ValueError(f'{filetype} file s3://{bucket}/{key} is empty.')
    df = pd.read_csv(StringIO(content.decode('utf-8')), header=None)
    if df.shape[1] > 1:
        df = df.iloc[:, [0]]
    df.columns = ['nhs']
    logger.info(f'Initial {filetype} count: {len(df)} in file {filename}')
    df = clean_and_validate_nhs_df(df, 'nhs')
    df = df.drop_duplicates(subset=['nhs'])
    logger.info(f'Cleaned, validated and deduplicated {filetype} count: {len(df)} in file {filename}')
    return df, filename


def lambda_handler(event, context) -> dict:
    try:
        # ENV variables
        sft_file_prefix = os.getenv("SFT_FILE_PATH")
        sft_checksum_prefix = os.getenv('SFT_CHECKSUM_PATH')
        gp_files_prefix = os.getenv('GP_FILES_PREFIX')
        gp_checksums_prefix = os.getenv('GP_CHECKSUMS_PREFIX')
        cohort_key = os.getenv('COHORT_KEY', 'cohort/cohort.csv')

        # SFT
        sft_bucket, sft_prefix = sft_file_prefix.split('/', 1)
        sft_files = list_s3_files(sft_bucket, sft_prefix)
        sft_key = sft_files[0]
        sft_checksum_bucket, sft_checksum_prefix = sft_checksum_prefix.split('/', 1)
        sft_checksum_files = list_s3_files(sft_checksum_bucket, sft_checksum_prefix)
        sft_sha_files = [f for f in sft_checksum_files if f.endswith('.sha256')]
        if len(sft_sha_files) != 1:
            raise FileNotFoundError(f'Expected exactly one SFT checksum file in s3://{sft_checksum_bucket}/{sft_checksum_prefix}, found: {sft_sha_files}')
        sft_checksum_key = sft_sha_files[0]
        sft_df, _ = load_and_clean_nhs_csv(sft_bucket, sft_key, sft_checksum_bucket, sft_checksum_key, filetype='SFT')

        # GP's
        gp_bucket, gp_prefix = gp_files_prefix.split('/', 1)
        gp_checksum_bucket, gp_checksum_prefix = gp_checksums_prefix.split('/', 1)
        gp_file_keys = sorted([k for k in list_s3_files(gp_bucket, gp_prefix)])
        intersections = []
        for gp_key in gp_file_keys:
            filename = gp_key.split('/')[-1]
            gp_checksum_key = f"{gp_checksum_prefix}{filename.replace('.csv', '.sha256')}"
            gp_df, _ = load_and_clean_nhs_csv(gp_bucket, gp_key, gp_checksum_bucket, gp_checksum_key, filetype='GP')
            intersection = pd.merge(sft_df, gp_df, on='nhs', how='inner')
            intersections.append(intersection)
            logger.info(f'Intersection {filename} count: {len(intersection)}')
        if intersections:
            all_common_df = pd.concat(intersections)
            logger.info(f'Union unique count before dedup: {len(all_common_df["nhs"])}')
            all_common_df = all_common_df.drop_duplicates()
        else:
            all_common_df = pd.DataFrame(columns=['nhs'])
            logger.warning('No intersections found, final union is empty.')
        logger.info(f'Final union count: {len(all_common_df)}')
        write_cohort(gp_bucket, cohort_key, set(all_common_df['nhs']))
        delete_and_log_remaining(sft_bucket, [sft_key], os.path.dirname(sft_key))
        delete_and_log_remaining(sft_checksum_bucket, [sft_checksum_key], os.path.dirname(sft_checksum_key))
        delete_and_log_remaining(gp_bucket, gp_file_keys, gp_prefix)
        gp_checksum_keys = [f"{gp_checksum_prefix}{key.split('/')[-1].replace('.csv', '.sha256')}" for key in gp_file_keys]
        delete_and_log_remaining(gp_checksum_bucket, gp_checksum_keys, gp_checksum_prefix)
        return {'final_count': len(all_common_df), 'cohort_key': cohort_key}
    except KeyError as e:
        logger.error(f'Missing or invalid environment variable: {e}', exc_info=True)
        raise
    except Exception as e:
        logger.error(f'Unhandled error in lambda_handler: {e}', exc_info=True)
        raise
