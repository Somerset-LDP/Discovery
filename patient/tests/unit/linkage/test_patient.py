"""
Tests for Patient cleaning functions.
"""
from datetime import date, timedelta
import pytest
import pandas as pd
import numpy as np

from linking.patient import clean_patient, Sex


# NHS number validation tests

def test_clean_patient_with_valid_nhs_number_with_spaces():
    """Test valid NHS number with spaces is standardized."""
    df = pd.DataFrame([{
        'nhs_number': '943 476 5919',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] == '9434765919'  # Spaces removed
    assert result.loc[0, 'dob'] == date(1980, 5, 15)


def test_clean_patient_with_invalid_nhs_number_checksum():
    """Test NHS number with invalid check digit is set to None."""
    df = pd.DataFrame([{
        'nhs_number': '9434765910',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] is None  # Invalid NHS number becomes None


def test_clean_patient_with_nhs_number_too_short():
    """Test NHS number with fewer than 10 digits is set to None."""
    df = pd.DataFrame([{
        'nhs_number': '943476591',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] is None


def test_clean_patient_with_nhs_number_too_long():
    """Test NHS number with more than 10 digits is set to None."""
    df = pd.DataFrame([{
        'nhs_number': '94347659199',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] is None


def test_clean_patient_with_non_numeric_nhs_number():
    """Test NHS number with non-numeric characters is set to None."""
    df = pd.DataFrame([{
        'nhs_number': 'abcdefghij',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] is None


def test_clean_patient_with_empty_nhs_number():
    """Test empty string NHS number is set to None."""
    df = pd.DataFrame([{
        'nhs_number': '',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] is None


# Postcode validation tests

@pytest.mark.parametrize("postcode,expected", [
    ("SW1A 1AA", "SW1A 1AA"),
    ("M1 1AA", "M1 1AA"),
    ("TA1 1AA", "TA1 1AA"),
    ("BS1 1AA", "BS1 1AA"),
    ("SW1A1AA", "SW1A 1AA"),  # Space added
    (" M1 1AA ", "M1 1AA"),  # Whitespace trimmed
])
def test_clean_patient_with_valid_postcodes(postcode, expected):
    """Test various valid UK postcode formats are standardized."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': 'Smith',
        'sex': 'male',
        'postcode': postcode,
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'postcode'] == expected


def test_clean_patient_with_invalid_postcode_format():
    """Test invalid postcode format is set to None."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': 'Smith',
        'sex': 'male',
        'postcode': 'INVALID',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'postcode'] is None


def test_clean_patient_with_empty_postcode():
    """Test empty string postcode is set to None."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': 'Smith',
        'sex': 'male',
        'postcode': '',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'postcode'] is None


# Name validation tests

def test_clean_patient_with_valid_names():
    """Test valid names are title-cased."""
    df = pd.DataFrame([{
        'first_name': 'john',
        'last_name': 'smith',
        'sex': 'male',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'first_name'] == 'John'
    assert result.loc[0, 'last_name'] == 'Smith'


def test_clean_patient_with_multi_word_names():
    """Test multi-word names are title-cased."""
    df = pd.DataFrame([{
        'first_name': 'mary jane',
        'last_name': 'smith-jones',
        'sex': 'female',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'first_name'] == 'Mary Jane'
    assert result.loc[0, 'last_name'] == 'Smith-Jones'


def test_clean_patient_with_empty_first_name():
    """Test empty first name is set to None."""
    df = pd.DataFrame([{
        'first_name': '',
        'last_name': 'Smith',
        'sex': 'male',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'first_name'] is None


def test_clean_patient_with_whitespace_first_name():
    """Test whitespace-only first name is set to None."""
    df = pd.DataFrame([{
        'first_name': '   ',
        'last_name': 'Smith',
        'sex': 'male',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'first_name'] is None


def test_clean_patient_with_empty_last_name():
    """Test empty last name is set to None."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': '',
        'sex': 'male',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'last_name'] is None


def test_clean_patient_with_whitespace_last_name():
    """Test whitespace-only last name is set to None."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': '   ',
        'sex': 'male',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'last_name'] is None


# Sex validation tests

def test_clean_patient_with_sex_standardization():
    """Test sex values are lowercased."""
    df = pd.DataFrame([{
        'first_name': 'John',
        'last_name': 'Smith',
        'sex': 'MALE',
        'postcode': 'SW1A 1AA',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'sex'] == 'male'


# Date of birth validation tests

def test_clean_patient_with_past_date_of_birth():
    """Test date in the past is preserved."""
    df = pd.DataFrame([{
        'nhs_number': '9434765919',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'dob'] == date(1980, 5, 15)


def test_clean_patient_with_today_date_of_birth():
    """Test today's date is preserved."""
    df = pd.DataFrame([{
        'nhs_number': '9434765919',
        'dob': date.today()
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'dob'] == date.today()


def test_clean_patient_with_future_date_of_birth():
    """Test date in the future is set to None."""
    future_date = date.today() + timedelta(days=365)
    df = pd.DataFrame([{
        'nhs_number': '9434765919',
        'dob': future_date
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'dob'] is None


def test_clean_patient_with_tomorrow_date_of_birth():
    """Test tomorrow's date is set to None."""
    tomorrow = date.today() + timedelta(days=1)
    df = pd.DataFrame([{
        'nhs_number': '9434765919',
        'dob': tomorrow
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'dob'] is None


# Multiple rows tests

def test_clean_patient_with_multiple_rows():
    """Test cleaning multiple patient rows."""
    df = pd.DataFrame([
        {
            'nhs_number': '943 476 5919',
            'dob': date(1980, 5, 15),
            'first_name': 'john',
            'last_name': 'smith',
            'sex': 'MALE',
            'postcode': 'SW1A1AA'
        },
        {
            'nhs_number': '8314495581',
            'dob': date(1990, 2, 20),
            'first_name': 'jane',
            'last_name': 'doe',
            'sex': 'FEMALE',
            'postcode': 'M1 1AA'
        }
    ])
    
    result = clean_patient(df)
    
    # First row
    assert result.loc[0, 'nhs_number'] == '9434765919'
    assert result.loc[0, 'first_name'] == 'John'
    assert result.loc[0, 'last_name'] == 'Smith'
    assert result.loc[0, 'sex'] == 'male'
    assert result.loc[0, 'postcode'] == 'SW1A 1AA'
    
    # Second row
    assert result.loc[1, 'nhs_number'] == '8314495581'
    assert result.loc[1, 'first_name'] == 'Jane'
    assert result.loc[1, 'last_name'] == 'Doe'
    assert result.loc[1, 'sex'] == 'female'
    assert result.loc[1, 'postcode'] == 'M1 1AA'


def test_clean_patient_with_mixed_valid_invalid_rows():
    """Test cleaning with mix of valid and invalid data."""
    df = pd.DataFrame([
        {
            'nhs_number': '9434765919',  # Valid
            'dob': date(1980, 5, 15)
        },
        {
            'nhs_number': '1234567890',  # Invalid checksum
            'dob': date(1990, 2, 20)
        }
    ])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] == '9434765919'
    assert result.loc[1, 'nhs_number'] is None


# NaN handling tests

def test_clean_patient_with_nan_values():
    """Test that NaN values are converted to None."""
    df = pd.DataFrame([{
        'nhs_number': '9434765919',
        'dob': date(1980, 5, 15),
        'first_name': np.nan,
        'last_name': 'Smith',
        'sex': np.nan,
        'postcode': np.nan
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] == '9434765919'
    assert result.loc[0, 'first_name'] is None
    assert result.loc[0, 'sex'] is None
    assert result.loc[0, 'postcode'] is None


# Empty DataFrame test

def test_clean_patient_with_empty_dataframe():
    """Test cleaning empty DataFrame returns empty DataFrame."""
    df = pd.DataFrame(columns=['nhs_number', 'dob', 'first_name', 'last_name', 'sex', 'postcode'])
    
    result = clean_patient(df)
    
    assert len(result) == 0
    assert list(result.columns) == ['nhs_number', 'dob', 'first_name', 'last_name', 'sex', 'postcode']


# Subset of columns test

def test_clean_patient_with_subset_of_columns():
    """Test cleaning DataFrame with only some columns present."""
    df = pd.DataFrame([{
        'nhs_number': '943 476 5919',
        'dob': date(1980, 5, 15)
    }])
    
    result = clean_patient(df)
    
    assert result.loc[0, 'nhs_number'] == '9434765919'
    assert result.loc[0, 'dob'] == date(1980, 5, 15)
    assert 'first_name' not in result.columns