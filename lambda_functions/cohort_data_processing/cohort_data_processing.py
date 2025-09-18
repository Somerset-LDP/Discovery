import os
import logging
import hashlib
import pandas as pd
from io import StringIO

from lambda_functions.cohort_data_processing.s3_utils import write_to_s3, delete_s3_objects, list_s3_files, \
    get_s3_object_content

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def validate_checksum(content, checksum_content, key):
    try:
        actual_checksum = hashlib.sha256(content).hexdigest()
        expected_checksum = checksum_content.decode('utf-8').strip()
        if actual_checksum != expected_checksum:
            logger.error(f'Checksum mismatch for {key}: expected {expected_checksum}, got {actual_checksum}')
            raise ValueError(f'Checksum mismatch for {key}')
    except Exception as e:
        logger.error(f'Error in checksum validation for {key}: {e}')
        raise


def write_cohort(gp_bucket, cohort_key, all_common):
    try:
        write_to_s3(gp_bucket, cohort_key, all_common)
    except Exception as e:
        logger.error(f'Error writing cohort file: {e}')
        raise


def delete_and_log_remaining(bucket, keys, prefix=None):
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


def is_valid_nhs_number(nhs_number) -> bool:
    # Accepts both string and numeric input, always works on string without spaces
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


def lambda_handler(event, context):
    try:
        sft_file = os.getenv("SFT_FILE_PATH")
        sft_checksum = os.getenv('SFT_CHECKSUM_PATH')
        gp_files_prefix = os.getenv('GP_FILES_PREFIX')
        gp_checksums_prefix = os.getenv('GP_CHECKSUMS_PREFIX')
        sft_bucket, sft_key = sft_file.split('/', 1)
        sft_checksum_bucket, sft_checksum_key = sft_checksum.split('/', 1)
        gp_bucket, gp_prefix = gp_files_prefix.split('/', 1)
        gp_checksum_bucket, gp_checksum_prefix = gp_checksums_prefix.split('/', 1)
    except KeyError as e:
        logger.error(f'Missing environment variable: {e}')
        raise

    # SFT
    sft_content = get_s3_object_content(sft_bucket, sft_key)
    sft_checksum_content = get_s3_object_content(sft_checksum_bucket, sft_checksum_key)
    validate_checksum(sft_content, sft_checksum_content, sft_key)
    # Read SFT as a single-column CSV, ignore index/row numbers if present
    sft_df = pd.read_csv(StringIO(sft_content.decode('utf-8')), header=None)
    # If more than one column, take only the first column as NHS number
    if sft_df.shape[1] > 1:
        sft_df = sft_df.iloc[:, [0]]
    sft_df.columns = ['nhs']
    logger.info(f'Initial SFT count: {len(sft_df)}')
    sft_df['nhs'] = sft_df['nhs'].astype(str).str.replace(' ', '').str.strip()
    sft_df = sft_df[sft_df['nhs'] != '']
    valid_mask = sft_df['nhs'].apply(is_valid_nhs_number)
    sft_df = sft_df[valid_mask]
    logger.info(f'Cleaned and validated SFT count: {len(sft_df)}')

    # GP's
    gp_file_keys = sorted([k for k in list_s3_files(gp_bucket, gp_prefix) if k.endswith('.csv')])
    intersections = []
    for gp_key in gp_file_keys:
        filename = gp_key.split('/')[-1]
        checksum_key = f"{gp_checksum_prefix}{filename.replace('.csv', '.sha256')}"
        gp_content = get_s3_object_content(gp_bucket, gp_key)
        gp_checksum_content = get_s3_object_content(gp_checksum_bucket, checksum_key)
        validate_checksum(gp_content, gp_checksum_content, gp_key)
        gp_df = pd.read_csv(StringIO(gp_content.decode('utf-8')), header=None, names=['nhs'])
        logger.info(f'GP file {filename} initial NHS count: {len(gp_df)}')
        gp_df['nhs'] = gp_df['nhs'].astype(str).str.replace(' ', '').str.strip()
        gp_df = gp_df[gp_df['nhs'] != '']
        valid_mask = gp_df['nhs'].apply(is_valid_nhs_number)
        gp_df = gp_df[valid_mask]
        logger.info(f'GP file {filename} cleaned and validated NHS count: {len(gp_df)}')
        intersection = pd.merge(sft_df, gp_df, on='nhs', how='inner')
        intersections.append(intersection)
        logger.info(f'Intersection {filename} count: {len(intersection)}')
    if intersections:
        all_common_df = pd.concat(intersections).drop_duplicates()
    else:
        all_common_df = pd.DataFrame(columns=['nhs'])
        logger.warning('No intersections found, final union is empty.')
    logger.info(f'Final union count: {len(all_common_df)}')

    cohort_key = 'cohort/cohort.csv'
    write_cohort(gp_bucket, cohort_key, set(all_common_df['nhs']))
    delete_and_log_remaining(sft_bucket, [sft_key], os.path.dirname(sft_key))
    delete_and_log_remaining(sft_checksum_bucket, [sft_checksum_key], os.path.dirname(sft_checksum_key))
    delete_and_log_remaining(gp_bucket, gp_file_keys, gp_prefix)
    gp_checksum_keys = [f"{gp_checksum_prefix}{key.split('/')[-1].replace('.csv', '.sha256')}" for key in gp_file_keys]
    delete_and_log_remaining(gp_checksum_bucket, gp_checksum_keys, gp_checksum_prefix)
    return {'final_count': len(all_common_df), 'cohort_key': cohort_key}
