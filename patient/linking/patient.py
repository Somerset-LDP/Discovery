from datetime import date
from enum import Enum
from typing import Optional
import pandas as pd
import re

class Sex(Enum):
    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    UNKNOWN = "unknown"

UK_POSTCODE_PATTERN = re.compile(
    r'^([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})$',
    re.IGNORECASE
)

# ============================================
# Combined standardization and validation functions
# ============================================

def _clean_nhs_number(nhs_number) -> Optional[str]:
    """Standardize and validate NHS number using Modulus 11 algorithm.
    
    Returns standardized NHS number if valid, None otherwise.
    """
    if nhs_number is None or pd.isna(nhs_number):
        return None
    
    # Standardize: remove spaces, ensure string
    nhs_clean = str(nhs_number).replace(' ', '').strip()
    
    # Validate: check format
    if not nhs_clean.isdigit() or len(nhs_clean) != 10:
        return None
    
    # Validate: Modulus 11 check
    digits = [int(d) for d in nhs_clean]
    total = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
    remainder = total % 11
    check_digit = 11 - remainder
    
    if check_digit == 11:
        check_digit = 0
    if check_digit == 10:
        return None
    
    if check_digit != digits[9]:
        return None
    
    return nhs_clean


def _clean_postcode(postcode) -> Optional[str]:
    """Standardize and validate UK postcode format.
    
    Returns standardized postcode in 'AA9 9AA' format if valid, None otherwise.
    """
    if postcode is None or pd.isna(postcode):
        return None
    
    if not isinstance(postcode, str):
        postcode = str(postcode)
    
    # Standardize: remove all spaces, uppercase, strip
    postcode = postcode.replace(' ', '').upper().strip()
    
    # Validate: check it's not empty
    if not postcode:
        return None
    
    # Insert space before last 3 characters
    if len(postcode) >= 5:
        formatted = f"{postcode[:-3]} {postcode[-3:]}"
    else:
        formatted = postcode
    
    # Validate: check against regex pattern
    if not UK_POSTCODE_PATTERN.match(formatted):
        return None
    
    return formatted


def _clean_name(name) -> Optional[str]:
    """Standardize and validate name (first or last).
    
    Returns title-cased name if valid, None otherwise.
    """
    if name is None or pd.isna(name):
        return None
    
    if not isinstance(name, str):
        name = str(name)
    
    # Standardize: strip and title case
    standardized = name.strip().title()
    
    # Validate: not empty after stripping
    if not standardized:
        return None
    
    return standardized


def _clean_sex(sex) -> Optional[str]:
    """Standardize and validate sex value.
    
    Returns lowercase sex string if valid, None otherwise.
    """
    if sex is None or pd.isna(sex):
        return None
    
    if isinstance(sex, Sex):
        return sex.value
    
    if not isinstance(sex, str):
        sex = str(sex)
    
    # Standardize: lowercase and strip
    return sex.lower().strip()


def _clean_dob(dob):
    """Standardize and validate date of birth.
    
    Returns DOB if not in the future, None otherwise.
    """
    if dob is None or pd.isna(dob):
        return None
    
    # Handle both datetime.date and pandas Timestamp
    if isinstance(dob, pd.Timestamp):
        dob_date = dob.date()
    else:
        dob_date = dob
    
    # Validate: not in the future
    if dob_date > date.today():
        return None
    
    return dob


# ============================================
# DataFrame manipulation functions
# ============================================

def clean_patient(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and validates patient DataFrame columns. Modifies the df param in-place."""

    if 'nhs_number' in df.columns:
        df['nhs_number'] = df['nhs_number'].apply(_clean_nhs_number)
    
    if 'postcode' in df.columns:
        df['postcode'] = df['postcode'].apply(_clean_postcode)
    
    if 'first_name' in df.columns:
        df['first_name'] = df['first_name'].apply(_clean_name)
    
    if 'last_name' in df.columns:
        df['last_name'] = df['last_name'].apply(_clean_name)

    if 'sex' in df.columns:
        df['sex'] = df['sex'].apply(_clean_sex)

    if 'dob' in df.columns:
        df['dob'] = df['dob'].apply(_clean_dob)

    # Convert NaN to None
    df = df.replace({pd.NA: None, float('nan'): None})
    
    return df