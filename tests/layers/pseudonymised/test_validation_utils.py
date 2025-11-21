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
    valid_sex_values = ['Male', 'Female', 'Indeterminate']
    assert is_valid_gender(gender, valid_sex_values) == expected


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
    validation_dob_format = "%d-%b-%y"
    assert is_valid_date_of_birth(date_of_birth, validation_dob_format) == expected


@pytest.mark.parametrize(
    "row_data,expected_valid,expected_error,validation_rules,fields_to_pseudonymise",
    [
        # GP Feed tests
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
                "",
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
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
                "Invalid NHS Number",
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
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
                "Invalid Given Name",
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
        ),
        # SFT Feed tests
        (
                {
                    'nhs_number': '9434765919',
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'date_of_birth': '1985-01-15',
                    'sex': '1',
                    'postcode': 'SW1A 1AA'
                },
                True,
                "",
                {
                    "valid_sex_values": ['1', '2', '9'],
                    "valid_date_format": "%Y-%m-%d"
                },
                {
                    'nhs_number': 'nhs_number',
                    'first_name': 'first_name',
                    'last_name': 'last_name',
                    'date_of_birth': 'date_of_birth',
                    'sex': 'sex',
                    'postcode': 'postcode'
                }
        ),
        (
                {
                    'nhs_number': '1234567890',
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'date_of_birth': '1985-01-15',
                    'sex': '1',
                    'postcode': 'SW1A 1AA'
                },
                False,
                "Invalid nhs_number",
                {
                    "valid_sex_values": ['1', '2', '9'],
                    "valid_date_format": "%Y-%m-%d"
                },
                {
                    'nhs_number': 'nhs_number',
                    'first_name': 'first_name',
                    'last_name': 'last_name',
                    'date_of_birth': 'date_of_birth',
                    'sex': 'sex',
                    'postcode': 'postcode'
                }
        ),
    ],
)
def test_validate_record(row_data, expected_valid, expected_error, validation_rules, fields_to_pseudonymise):
    row = pd.Series(row_data)
    is_valid, error_message = validate_record(row, validation_rules, fields_to_pseudonymise)
    assert is_valid == expected_valid
    assert error_message == expected_error


@pytest.mark.parametrize(
    "df_data,expected_valid_count,expected_invalid_count,validation_rules,fields_to_pseudonymise",
    [
        # GP Feed - all valid
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
                0,
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
        ),
        # GP Feed - one invalid NHS
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
                1,
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
        ),
        # GP Feed - all invalid names
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
                2,
                {
                    "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
                    "valid_date_format": "%d-%b-%y"
                },
                {
                    'NHS Number': 'nhs_number',
                    'Given Name': 'given_name',
                    'Family Name': 'family_name',
                    'Date of Birth': 'date_of_birth',
                    'Gender': 'gender',
                    'Postcode': 'postcode'
                }
        ),
        # SFT Feed - all valid
        (
                [
                    {
                        'nhs_number': '9434765919',
                        'first_name': 'John',
                        'last_name': 'Doe',
                        'date_of_birth': '1985-01-15',
                        'sex': '1',
                        'postcode': 'SW1A 1AA'
                    },
                    {
                        'nhs_number': '8314495581',
                        'first_name': 'Jane',
                        'last_name': 'Smith',
                        'date_of_birth': '1990-02-20',
                        'sex': '2',
                        'postcode': 'M1 1AA'
                    },
                ],
                2,
                0,
                {
                    "valid_sex_values": ['1', '2', '9'],
                    "valid_date_format": "%Y-%m-%d"
                },
                {
                    'nhs_number': 'nhs_number',
                    'first_name': 'first_name',
                    'last_name': 'last_name',
                    'date_of_birth': 'date_of_birth',
                    'sex': 'sex',
                    'postcode': 'postcode'
                }
        ),
        # SFT Feed - one invalid NHS
        (
                [
                    {
                        'nhs_number': '1234567890',
                        'first_name': 'John',
                        'last_name': 'Doe',
                        'date_of_birth': '1985-01-15',
                        'sex': '1',
                        'postcode': 'SW1A 1AA'
                    },
                    {
                        'nhs_number': '8314495581',
                        'first_name': 'Jane',
                        'last_name': 'Smith',
                        'date_of_birth': '1990-02-20',
                        'sex': '2',
                        'postcode': 'M1 1AA'
                    },
                ],
                1,
                1,
                {
                    "valid_sex_values": ['1', '2', '9'],
                    "valid_date_format": "%Y-%m-%d"
                },
                {
                    'nhs_number': 'nhs_number',
                    'first_name': 'first_name',
                    'last_name': 'last_name',
                    'date_of_birth': 'date_of_birth',
                    'sex': 'sex',
                    'postcode': 'postcode'
                }
        ),
    ],
)
def test_validate_dataframe(df_data, expected_valid_count, expected_invalid_count, validation_rules, fields_to_pseudonymise):
    df = pd.DataFrame(df_data)
    valid_df, invalid_records = validate_dataframe(df, validation_rules, fields_to_pseudonymise)
    assert len(valid_df) == expected_valid_count
    assert len(invalid_records) == expected_invalid_count
