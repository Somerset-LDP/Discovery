import os
import logging
import hashlib
import pandas as pd
from io import StringIO
from typing import List, Set, Union

from botocore.exceptions import BotoCoreError, ClientError

from lambda_functions.cohort_data_processing.aws_utils import (
    write_to_s3,
    delete_s3_objects,
    list_s3_files,
    get_s3_object_content,
    invoke_lambda
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENCODING = "utf-8"
NHS_NUMBER_COLUMN = "nhs"
FILE_EXTENSION = ".csv"
CHECKSUM_EXTENSION = ".sha256"
REQUIRED_ENV_VARS = [
    "S3_SFT_FILE_PREFIX",
    "S3_SFT_CHECKSUM_PREFIX",
    "S3_GP_FILES_PREFIX",
    "S3_GP_CHECKSUMS_PREFIX",
    "S3_COHORT_KEY",
    "KMS_KEY_ID",
    "PSEUDONYMISATION_LAMBDA_FUNCTION_NAME"
]


def get_files(file_prefix: str) -> (str, List[str]):
    bucket, prefix = file_prefix.split('/', 1)
    files = list_s3_files(bucket, prefix)
    if not files:
        logger.error(f'No files found in s3://{bucket}/{prefix}. Aborting lambda execution.')
        raise ValueError(f'No files found in s3://{bucket}/{prefix}.')
    return bucket, files


def validate_checksum(content: bytes, checksum_content: bytes, key: str) -> None:
    try:
        # Allow sha256sum-style files: "<hex>  filename"
        expected_checksum = checksum_content.decode(ENCODING).strip().split()[0]
    except UnicodeDecodeError as e:
        logger.error(f"Checksum decode failed for {key}: {e}")
        raise
    actual_checksum = hashlib.sha256(content).hexdigest()
    if actual_checksum != expected_checksum:
        msg = f"Checksum mismatch for {key}: expected {expected_checksum}, got {actual_checksum}"
        logger.error(msg)
        raise ValueError(msg)


def delete_and_log_remaining(bucket: str, keys: List[str], prefix: str) -> None:
    delete_s3_objects(bucket, keys)
    remaining = list_s3_files(bucket, prefix)
    if remaining:
        logger.warning(
            f'Not all files deleted in s3://{bucket}/{prefix}. Remaining: {remaining} (count: {len(remaining)})')
    else:
        logger.info(f'All source files deleted in s3://{bucket}/{prefix}')


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


def clean_and_validate_nhs_df(df: pd.DataFrame, nhs_col: str) -> pd.DataFrame:
    df[nhs_col] = df[nhs_col].astype(str).str.replace(' ', '').str.strip()
    df = df[df[nhs_col] != '']
    validation_result = df[nhs_col].apply(is_valid_nhs_number)
    return df[validation_result]


def load_and_clean_nhs_csv(
        bucket: str, key: str, checksum_bucket: str, checksum_key: str, filetype: str
) -> pd.DataFrame:
    content = get_s3_object_content(bucket, key)
    checksum_content = get_s3_object_content(checksum_bucket, checksum_key)
    validate_checksum(content, checksum_content, key)
    if not content.strip():
        logger.error(f'{filetype} file s3://{bucket}/{key} is empty. Aborting lambda execution.')
        raise ValueError(f'{filetype} file s3://{bucket}/{key} is empty.')
    df = pd.read_csv(StringIO(content.decode(ENCODING)), header=None)
    if df.shape[1] > 1:
        df = df.iloc[:, [0]]
    df.columns = [NHS_NUMBER_COLUMN]
    filename = key.split('/')[-1]
    logger.info(f'Initial {filetype} count: {len(df)} in file {filename}')
    df = clean_and_validate_nhs_df(df, NHS_NUMBER_COLUMN)
    df = df.drop_duplicates(subset=[NHS_NUMBER_COLUMN])
    logger.info(f'Cleaned, validated and deduplicated {filetype} count: {len(df)} in file {filename}')
    return df


def pseudonymise_nhs_numbers(nhs_numbers: Set[str], lambda_function_name: str) -> Set[str]:
    if not nhs_numbers:
        logger.warning("Empty set provided for pseudonymisation")
        return set()

    logger.info(f"Starting pseudonymisation of {len(nhs_numbers)} NHS numbers")
    nhs_list = sorted(list(nhs_numbers))
    payload = {
        "action": "encrypt",
        "field_name": "nhs_number",
        "field_value": nhs_list
    }
    response_payload = invoke_lambda(lambda_function_name, payload)

    if 'error' in response_payload:
        error_msg = f"Pseudonymisation Lambda returned error: {response_payload['error']}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    if 'field_value' not in response_payload:
        error_msg = "Pseudonymisation Lambda response missing 'field_value'"
        logger.error(error_msg)
        raise ValueError(error_msg)

    pseudonymised_values = response_payload['field_value']
    if len(pseudonymised_values) != len(nhs_list):
        error_msg = f"Pseudonymisation returned {len(pseudonymised_values)} values, expected {len(nhs_list)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    pseudonymised_set = set(pseudonymised_values)
    logger.info(f"Successfully pseudonymised {len(pseudonymised_set)} NHS numbers")
    return pseudonymised_set


def get_env_variables() -> dict:
    env_vars = {var: os.getenv(var, '').strip() for var in REQUIRED_ENV_VARS}
    missing = [var for var, val in env_vars.items() if not val]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        raise KeyError(f"Missing required environment variables: {', '.join(missing)}")
    return env_vars


def lambda_handler(event, context) -> dict:
    try:
        # ENV variables
        env_vars = get_env_variables()
        sft_file_prefix = env_vars["S3_SFT_FILE_PREFIX"]
        sft_checksum_prefix = env_vars["S3_SFT_CHECKSUM_PREFIX"]
        gp_files_prefix = env_vars["S3_GP_FILES_PREFIX"]
        gp_checksums_prefix = env_vars["S3_GP_CHECKSUMS_PREFIX"]
        cohort_path = env_vars["S3_COHORT_KEY"]
        kms_key_id = env_vars["KMS_KEY_ID"]
        pseudonymisation_lambda = env_vars["PSEUDONYMISATION_LAMBDA_FUNCTION_NAME"]

        # SFT
        sft_bucket, sft_keys = get_files(sft_file_prefix)
        sft_checksum_bucket, sft_checksum_keys = get_files(sft_checksum_prefix)
        sft_file_key = sft_keys[0]
        sft_checksum_key = sft_checksum_keys[0]
        sft_df = load_and_clean_nhs_csv(sft_bucket, sft_file_key, sft_checksum_bucket, sft_checksum_key, filetype='SFT')
        sft_set = set(sft_df[NHS_NUMBER_COLUMN])

        # GP's
        gp_bucket, gp_file_keys = get_files(gp_files_prefix)
        gp_checksum_bucket, gp_checksum_keys = get_files(gp_checksums_prefix)

        if len(gp_file_keys) != len(gp_checksum_keys):
            logger.error('Mismatch between number of GP files and checksum files. Aborting lambda execution.')
            raise ValueError('Mismatch between number of GP files and checksum files.')

        gp_checksum_prefix = gp_checksums_prefix.split('/', 1)[1]
        logger.info(f'Found {len(gp_file_keys)} GP files to process.')

        # Intersection per GP file
        intersections = []
        for gp_key in gp_file_keys:
            filename = gp_key.split("/")[-1]
            gp_checksum_key = f"{gp_checksum_prefix}{filename.replace(FILE_EXTENSION, CHECKSUM_EXTENSION)}"
            gp_df = load_and_clean_nhs_csv(gp_bucket, gp_key, gp_checksum_bucket, gp_checksum_key, filetype="GP")

            gp_set = set(gp_df[NHS_NUMBER_COLUMN])
            common = sft_set & gp_set

            intersections.append(common)
            logger.info(f"Intersection {filename} count: {len(common)}")

        # Union of all intersections
        all_common = set()
        if not intersections:
            logger.warning('No intersections found, final union is empty.')
        else:
            all_common = set().union(*intersections)
        logger.info(f'Final union count: {len(all_common)}')

        # Pseudonymise cohort
        pseudonymised_cohort = pseudonymise_nhs_numbers(
            all_common,
            pseudonymisation_lambda
        )

        # Write cohort
        cohort_bucket, cohort_key = cohort_path.split('/', 1)
        write_to_s3(cohort_bucket, cohort_key, pseudonymised_cohort, kms_key_id)

        # Cleanup
        delete_and_log_remaining(sft_bucket, [sft_file_key], os.path.dirname(sft_file_key))
        delete_and_log_remaining(sft_checksum_bucket, [sft_checksum_key], os.path.dirname(sft_checksum_key))
        delete_and_log_remaining(gp_bucket, gp_file_keys, gp_files_prefix.split('/', 1)[1])
        delete_and_log_remaining(gp_checksum_bucket, gp_checksum_keys, gp_checksum_prefix)

        return {
            'final_count': len(all_common),
            'pseudonymised_count': len(pseudonymised_cohort),
            'cohort_key': cohort_key
        }

    except (LookupError, ValueError, UnicodeError, BotoCoreError, ClientError):
        raise
    except Exception as e:
        logger.error(f'Unhandled error in lambda_handler: {e}', exc_info=True)
        raise
