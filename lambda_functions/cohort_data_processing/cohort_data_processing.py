import os
import csv
import logging
import hashlib

from lambda_functions.cohort_data_processing.s3_utils import write_to_s3, delete_s3_objects, list_s3_files, \
    get_s3_object_content

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


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


def find_duplicates(items):
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
        else:
            seen.add(item)
    return duplicates


def read_csv_content(content, key, skip_header=False):
    try:
        decoded = content.decode('utf-8').strip()
        if not decoded:
            logger.error(f'File {key} is empty')
            raise ValueError(f'File {key} is empty')
        reader = csv.reader(decoded.splitlines())
        nhs_numbers_list = []
        for i, row in enumerate(reader):
            if skip_header and i == 0:
                continue
            if len(row) != 1:
                logger.warning(f'Invalid CSV format in {key} at line {i+1}: {row}')
                continue
            nhs_numbers_list.append(row[0].strip())
        # Check for duplicates
        duplicates = find_duplicates(nhs_numbers_list)
        if duplicates:
            logger.warning(f'Found {len(duplicates)} duplicate NHS numbers in {key}')
        # Validate NHS numbers
        valid_nhs_numbers = [nhs for nhs in nhs_numbers_list if is_valid_nhs_number(nhs)]
        invalid_count = len(nhs_numbers_list) - len(valid_nhs_numbers)
        if invalid_count > 0:
            logger.warning(f'Found {invalid_count} invalid NHS numbers in {key}')
        nhs_numbers = set(valid_nhs_numbers)
        if not nhs_numbers:
            logger.warning(f'No valid NHS numbers found in {key}')
        return nhs_numbers
    except Exception as e:
        logger.error(f'Error reading csv content for {key}: {e}')
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


def is_valid_nhs_number(nhs_number: str) -> bool:
    nhs_number = nhs_number.strip()
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
    sft_nhs_numbers = read_csv_content(sft_content, sft_key)
    logger.info(f'Initial SFT count: {len(sft_nhs_numbers)}')

    # GP
    gp_file_keys = sorted([k for k in list_s3_files(gp_bucket, gp_prefix) if k.endswith('.csv')])
    intersections = {}
    for gp_key in gp_file_keys:
        filename = gp_key.split('/')[-1]
        checksum_key = f"{gp_checksum_prefix}{filename.replace('.csv', '.sha256')}"
        gp_content = get_s3_object_content(gp_bucket, gp_key)
        gp_checksum_content = get_s3_object_content(gp_checksum_bucket, checksum_key)
        validate_checksum(gp_content, gp_checksum_content, gp_key)
        gp_nhs_numbers = read_csv_content(gp_content, gp_key)
        intersection = sft_nhs_numbers & gp_nhs_numbers
        intersections[filename] = intersection
        logger.info(f'Intersection {filename} count: {len(intersection)}')
    if intersections:
        all_common = set.union(*intersections.values())
    else:
        all_common = set()
        logger.warning('No intersections found, final union is empty.')
    logger.info(f'Final union count: {len(all_common)}')

    cohort_key = 'cohort/cohort.csv'
    write_cohort(gp_bucket, cohort_key, all_common)
    delete_and_log_remaining(sft_bucket, [sft_key], os.path.dirname(sft_key))
    delete_and_log_remaining(sft_checksum_bucket, [sft_checksum_key], os.path.dirname(sft_checksum_key))
    delete_and_log_remaining(gp_bucket, gp_file_keys, gp_prefix)
    gp_checksum_keys = [f"{gp_checksum_prefix}/{key.split('/')[-1].replace('.csv', '.sha256')}" for key in gp_file_keys]
    delete_and_log_remaining(gp_checksum_bucket, gp_checksum_keys, gp_checksum_prefix)
    return {'final_count': len(all_common), 'cohort_key': cohort_key}
