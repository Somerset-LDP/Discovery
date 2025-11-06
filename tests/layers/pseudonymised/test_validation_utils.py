import pandas as pd
import pytest

from validation_utils import (
    is_valid_string,
    is_valid_gender,
    is_valid_nhs_number,
    is_valid_uk_postcode,
    is_valid_date_of_birth,
    validate_record,
    validate_dataframe
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("valid string", True),
        (None, False),
        ("", False),
    ],
)
def test_is_valid_string(value, expected):
    assert is_valid_string(value) == expected


@pytest.mark.parametrize(
    "gender,expected",
    [
        ("Male", True),
        ("Female", True),
        ("Indeterminate", True),
        ("Unknown", False),
        (None, False),
    ],
)
def test_is_valid_gender(gender, expected):
    assert is_valid_gender(gender) == expected


@pytest.mark.parametrize(
    "nhs_number,expected",
    [
        ("9434765919", True),
        ("1234567890", False),
        (None, False),
    ],
)
def test_is_valid_nhs_number(nhs_number, expected):
    assert is_valid_nhs_number(nhs_number) == expected


@pytest.mark.parametrize(
    "postcode,expected",
    [
        ("SW1A 1AA", True),
        ("INVALID", False),
        (None, False),
    ],
)
def test_is_valid_uk_postcode(postcode, expected):
    assert is_valid_uk_postcode(postcode) == expected


@pytest.mark.parametrize(
    "date_of_birth,expected",
    [
        ("15-Jan-85", True),
        ("invalid-date", False),
        (None, False),
    ],
)
def test_is_valid_date_of_birth(date_of_birth, expected):
    assert is_valid_date_of_birth(date_of_birth) == expected


@pytest.mark.parametrize(
    "row_data,expected_valid,expected_error",
    [
        (
                {
                    'NHS Number': '9434765919',
                    'Given Name': 'John',
                    'Family Name': 'Doe',
                    'Date of Birth': '15-Jan-85',
                    'Gender': 'Male',
                    'Postcode': 'SW1A 1AA'
                },
                True,
                ""
        ),
        (
                {
                    'NHS Number': '1234567890',
                    'Given Name': 'John',
                    'Family Name': 'Doe',
                    'Date of Birth': '15-Jan-85',
                    'Gender': 'Male',
                    'Postcode': 'SW1A 1AA'
                },
                False,
                "Invalid NHS Number"
        ),
        (
                {
                    'NHS Number': '9434765919',
                    'Given Name': None,
                    'Family Name': 'Doe',
                    'Date of Birth': '15-Jan-85',
                    'Gender': 'Male',
                    'Postcode': 'SW1A 1AA'
                },
                False,
                "Invalid Given Name"
        ),
    ],
)
def test_validate_record(row_data, expected_valid, expected_error):
    row = pd.Series(row_data)
    is_valid, error_message = validate_record(row)
    assert is_valid == expected_valid
    assert error_message == expected_error


@pytest.mark.parametrize(
    "df_data,expected_valid_count,expected_invalid_count",
    [
        (
                [
                    {
                        'NHS Number': '9434765919',
                        'Given Name': 'John',
                        'Family Name': 'Doe',
                        'Date of Birth': '15-Jan-85',
                        'Gender': 'Male',
                        'Postcode': 'SW1A 1AA'
                    },
                    {
                        'NHS Number': '8314495581',
                        'Given Name': 'Jane',
                        'Family Name': 'Smith',
                        'Date of Birth': '20-Feb-90',
                        'Gender': 'Female',
                        'Postcode': 'M1 1AA'
                    },
                ],
                2,
                0
        ),
        (
                [
                    {
                        'NHS Number': '1234567890',
                        'Given Name': 'John',
                        'Family Name': 'Doe',
                        'Date of Birth': '15-Jan-85',
                        'Gender': 'Male',
                        'Postcode': 'SW1A 1AA'
                    },
                    {
                        'NHS Number': '8314495581',
                        'Given Name': 'Jane',
                        'Family Name': 'Smith',
                        'Date of Birth': '20-Feb-90',
                        'Gender': 'Female',
                        'Postcode': 'M1 1AA'
                    },
                ],
                1,
                1
        ),
        (
                [
                    {
                        'NHS Number': '9434765919',
                        'Given Name': None,
                        'Family Name': 'Doe',
                        'Date of Birth': '15-Jan-85',
                        'Gender': 'Male',
                        'Postcode': 'SW1A 1AA'
                    },
                    {
                        'NHS Number': '8314495581',
                        'Given Name': 'Jane',
                        'Family Name': '',
                        'Date of Birth': '20-Feb-90',
                        'Gender': 'Female',
                        'Postcode': 'M1 1AA'
                    },
                ],
                0,
                2
        ),
    ],
)
def test_validate_dataframe(df_data, expected_valid_count, expected_invalid_count):
    df = pd.DataFrame(df_data)
    valid_df, invalid_records = validate_dataframe(df)
    assert len(valid_df) == expected_valid_count
    assert len(invalid_records) == expected_invalid_count
