import logging
import re
from typing import Union, Tuple, List, Dict, Any

import pandas as pd

logger = logging.getLogger()

UK_POSTCODE_PATTERN = re.compile(
    r'^([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})$',
    re.IGNORECASE
)


def is_valid_string(value: Union[str, None]) -> bool:
    if value is None:
        return False
    if not isinstance(value, str):
        return False
    return bool(value.strip())


def is_valid_gender(gender: Union[str, None], valid_sex_values: List[str]) -> bool:
    if not is_valid_string(gender):
        return False

    return gender.strip() in valid_sex_values


def is_valid_nhs_number(nhs_number: Union[str, int, None]) -> bool:
    if nhs_number is None:
        return False

    nhs_number = str(nhs_number).replace(' ', '').strip()

    if not nhs_number.isdigit() or len(nhs_number) != 10:
        return False

    digits = [int(d) for d in nhs_number]

    total = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
    remainder = total % 11
    check_digit = 11 - remainder

    if check_digit == 11:
        check_digit = 0
    if check_digit == 10:
        return False

    return check_digit == digits[9]


def is_valid_uk_postcode(postcode: Union[str, None]) -> bool:
    """
    Format: A9 9AA, A99 9AA, AA9 9AA, AA99 9AA, A9A 9AA, AA9A 9AA
    """
    if not is_valid_string(postcode):
        return False

    return UK_POSTCODE_PATTERN.match(postcode.strip()) is not None


def is_valid_date_of_birth(date_of_birth: Union[str, None], validation_dob_format: str) -> bool:
    if not is_valid_string(date_of_birth):
        return False

    try:
        pd.to_datetime(date_of_birth.strip(), format=validation_dob_format, errors='raise')
        return True
    except (ValueError, TypeError):
        return False


def validate_record(
        row: pd.Series,
        validation_rules: Dict[str, Any], 
        fields_to_pseudonymise: Dict[str, str]
) -> Tuple[bool, str]:
    for column_name, field_type in fields_to_pseudonymise.items():
        value = row.get(column_name)

        if field_type == 'nhs_number':
            if not is_valid_nhs_number(value):
                return False, f"Invalid {column_name}"

        elif field_type in ('given_name', 'first_name', 'family_name', 'last_name'):
            if not is_valid_string(value):
                return False, f"Invalid {column_name}"

        elif field_type == 'date_of_birth':
            if not is_valid_date_of_birth(value, validation_rules.get("valid_date_format")):
                return False, f"Invalid {column_name}"

        elif field_type in ('gender', 'sex'):
            if not is_valid_gender(value, validation_rules.get("valid_sex_values")):
                return False, f"Invalid {column_name}"

        elif field_type == 'postcode':
            if column_name in row.index and not is_valid_uk_postcode(value):
                return False, f"Invalid {column_name}"

    return True, ""


def validate_dataframe(
        df: pd.DataFrame,
        validation_rules: Dict[str, Any],
        fields_to_pseudonymise: Dict[str, str]
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    initial_count = len(df)
    logger.info(f"Starting validation of {initial_count} records")

    required_columns = list(fields_to_pseudonymise.keys())
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    valid_indices = []
    invalid_records = []

    for idx, row in df.iterrows():
        is_valid, error_message = validate_record(row, validation_rules, fields_to_pseudonymise)

        if is_valid:
            valid_indices.append(idx)
        else:
            invalid_records.append({
                'row_index': idx,
                'error': error_message
            })

    valid_df = df.loc[valid_indices].copy()
    valid_count = len(valid_df)
    invalid_count = len(invalid_records)

    logger.info(f"Validation complete: {valid_count} valid records, {invalid_count} invalid records removed")

    if invalid_count > 0:
        error_summary = {}
        for record in invalid_records:
            error_type = record['error']
            error_summary[error_type] = error_summary.get(error_type, 0) + 1
        logger.warning(f"Removed {invalid_count} invalid records. Error breakdown: {error_summary}")

    return valid_df, invalid_records
