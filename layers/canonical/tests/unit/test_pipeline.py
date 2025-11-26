import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pipeline.canonical_processor import run

# Expected single-level columns based on GP feed
expected_columns = [
    'NHS Number', 'First name', 'Last name', 'Date of birth', 'Postcode', 
    'First line of address', 'Gender',
    'Value', 'Unit of Measure', 'Date',  # Height measurements
    'Value.1', 'Unit of Measure.1', 'Date.1',  # Weight measurements  
    'Consultation ID', 'Date.2', 'Time', 'Type of Consultation', "User Details' User Type"
]

# Test Patient Data - Global Variables
VALID_PATIENT_JOHN = [
    '111 222 3333',     # NHS Number
    'John',             # First name
    'Doe',              # Last name
    '01-Jan-50',        # Date of birth
    'AB1 2CD',          # Postcode
    '123 Test Street',  # First line of address
    'Male',             # Gender
    '175',              # Height Value
    'cm',               # Height Unit
    '27-Jun-25',        # Height Date
    '75',               # Weight Value
    'kg',               # Weight Unit
    '27-Jun-25',        # Weight Date
    '12345',            # Consultation ID
    '15-May-25',        # Consultation Date
    '09:30',            # Consultation Time
    'Face to face',     # Type of Consultation
    'GP'                # User Type
]

VALID_PATIENT_JANE = [
    '222 333 4444',
    'Jane',
    'Smith',
    '01-Jan-60',
    'CD2 3EF',
    '456 Main St',
    'Female',
    '165',
    'cm',
    '27-Jun-25',
    '65',
    'kg',
    '27-Jun-25',
    '12346',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

VALID_PATIENT_BOB = [
    '333 444 5555',
    'Bob',
    'Johnson',
    '01-Jan-70',
    'EF3 4GH',
    '789 Oak Ave',
    'Male',
    '180',
    'cm',
    '27-Jun-25',
    '80',
    'kg',
    '27-Jun-25',
    '12347',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_EMPTY_NHS = [
    '',                 # Empty NHS Number - should cause validation failure
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    '175',
    'cm',
    '27-Jun-25',
    '75',
    'kg',
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_EMPTY_DETAILS = [
    '', '', '', '', '', '', '',  # Empty patient details
    '180',
    'cm',
    '27-Jun-25',
    '80',
    'kg',
    '27-Jun-25',
    '12346',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_WRONG_HEIGHT_UNITS = [
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    '5.8',              # Height value
    'ft',               # Wrong units - should be 'cm'
    '27-Jun-25',
    '75',
    'kg',
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_NON_NUMERIC_HEIGHT = [
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    'tall',             # Non-numeric height value
    'cm',
    '27-Jun-25',
    '75',
    'kg',
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_WRONG_WEIGHT_UNITS = [
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    '175',
    'cm',
    '27-Jun-25',
    '165',              # Weight value
    'lbs',              # Wrong units - should be 'kg'
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

VALID_PATIENT_EMPTY_WEIGHT = [
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    '175',
    'cm',
    '27-Jun-25',
    '',                 # Empty weight value - should be allowed
    'kg',
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_INVALID_DATE_FORMAT = [
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'AB1 2CD',
    '123 Test Street',
    'Male',
    '175',
    'cm',
    '2025-06-27',       # Invalid date format - should be dd-MMM-yy
    '75',
    'kg',
    '27-Jun-25',
    '12345',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

INVALID_PATIENT_NON_NUMERIC_WEIGHT = [
    '333 444 5555',
    'Bob',
    'Johnson',
    '01-Jan-70',
    'EF3 4GH',
    '789 Oak Ave',
    'Male',
    '180',
    'cm',
    '27-Jun-25',
    'heavy',            # Non-numeric weight - should cause validation failure
    'kg',
    '27-Jun-25',
    '12347',
    '15-May-25',
    '09:30',
    'Face to face',
    'GP'
]

# SFT Feed Test Data
sft_expected_columns = [
    'pas_number', 'nhs_number', 'first_name', 'last_name', 'date_of_birth',
    'sex', 'postcode', 'first_line_of_address'
]

VALID_SFT_PATIENT_JOHN = [
    'PAS12345',
    '111 222 3333',
    'John',
    'Doe',
    '1950-01-01',
    'Male',
    'AB1 2CD',
    '123 Test Street',
]

VALID_SFT_PATIENT_JANE = [
    'PAS12346',
    '222 333 4444',
    'Jane',
    'Smith',
    '1960-01-01',
    'Female',
    'CD2 3EF',
    '456 Main St',
]

INVALID_SFT_PATIENT_EMPTY_NHS = [
    'PAS12345',
    '',
    'John',
    'Doe',
    '1950-01-01',
    'Male',
    'AB1 2CD',
    '123 Test Street',
]

INVALID_SFT_PATIENT_WRONG_DATE_FORMAT = [
    'PAS12345',
    '111 222 3333',
    'John',
    'Doe',
    '01-Jan-50',
    'Male',
    'AB1 2CD',
    '123 Test Street',
]

# Helper functions for DataFrame creation
def create_test_dataframe(*patients, columns=None):
    """Create a test DataFrame with the given patient records"""
    if columns is None:
        columns = expected_columns
    df = pd.DataFrame(columns=columns)
    for i, patient in enumerate(patients):
        df.loc[i] = patient
    return df

def create_single_patient_df(patient_data, columns=None):
    """Create DataFrame with a single patient record"""
    return create_test_dataframe(patient_data, columns=columns)


def test_column_structure_validation_failure():
    """Test that the method raises ValueError when input DataFrame has incorrect columns"""
    # Create DataFrame with wrong columns
    wrong_columns = [
        ('Wrong', 'Column1'),
        ('Wrong', 'Column2'),
    ]
    df = pd.DataFrame(columns=pd.MultiIndex.from_tuples(wrong_columns))
    
    with pytest.raises(ValueError, match="Input data does not have enough columns for GP feed structure"):
        run(df, feed_type="gp")


def test_valid_complete_records_processing():
    """Test successful processing of valid complete records"""
    df = create_single_patient_df(VALID_PATIENT_JOHN)
    
    result = run(df, feed_type="gp")

    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1112223333'  # Spaces removed in processing
    assert result.iloc[0]['given_name'] == 'John'
    assert result.iloc[0]['family_name'] == 'Doe'
    assert result.iloc[0]['height_cm'] == '175'
    assert result.iloc[0]['weight_kg'] == '75'
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]


def test_empty_input_dataframe_handling():
    """Test handling of empty DataFrame with correct structure"""
    # Create empty DataFrame with correct columns
    df = pd.DataFrame(columns=expected_columns)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]


@patch('pipeline.canonical_processor.logging.getLogger')
def test_mixed_valid_and_invalid_records(mock_logger):
    """Test processing of mixed valid and invalid records"""
    mock_logger_instance = MagicMock()
    mock_logger.return_value = mock_logger_instance
    
    df = create_test_dataframe(VALID_PATIENT_JOHN, INVALID_PATIENT_EMPTY_DETAILS)
    
    result = run(df, feed_type="gp")

    # Only valid record should be in result
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1112223333'  # Spaces removed in processing
    
    # Check that warning was logged for skipped record
    mock_logger_instance.warning.assert_any_call("Record at index 1 failed validation and will be skipped")


def test_patient_details_validation_failure():
    """Test rejection of records with missing patient details"""
    df = create_single_patient_df(INVALID_PATIENT_EMPTY_NHS)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0  # Record should be rejected


def test_height_measurement_validation_wrong_units():
    """Test rejection of records with invalid height measurements"""
    df = create_single_patient_df(INVALID_PATIENT_WRONG_HEIGHT_UNITS)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0  # Record should be rejected


def test_height_measurement_validation_non_numeric():
    """Test rejection of records with non-numeric height values"""
    df = create_single_patient_df(INVALID_PATIENT_NON_NUMERIC_HEIGHT)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0  # Record should be rejected


def test_weight_measurement_validation_wrong_units():
    """Test rejection of records with invalid weight measurements"""
    df = create_single_patient_df(INVALID_PATIENT_WRONG_WEIGHT_UNITS)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0  # Record should be rejected


def test_weight_measurement_validation_missing_value():
    """Test rejection of records with missing weight values"""
    df = create_single_patient_df(VALID_PATIENT_EMPTY_WEIGHT)
    
    result = run(df, feed_type="gp")

    assert len(result) == 1  # Record should be accepted (empty weight is allowed)
    assert result.iloc[0]['weight_kg'] == ""  # Weight should remain empty


def test_date_format_validation_invalid_format():
    """Test rejection of records with invalid date formats"""
    df = create_single_patient_df(INVALID_PATIENT_INVALID_DATE_FORMAT)
    
    result = run(df, feed_type="gp")

    assert len(result) == 0  # Record should be rejected


def test_date_format_validation_valid_format():
    """Test acceptance of records with valid date formats"""
    df = create_single_patient_df(VALID_PATIENT_JOHN)
    
    result = run(df, feed_type="gp")

    assert len(result) == 1  # Record should be accepted
    assert result.iloc[0]['height_observation_time'] == '27-Jun-25'
    assert result.iloc[0]['weight_observation_time'] == '27-Jun-25'


def test_all_records_invalid_scenario():
    """Test handling when all input records fail validation"""
    df = create_test_dataframe(
        INVALID_PATIENT_EMPTY_DETAILS,
        INVALID_PATIENT_WRONG_HEIGHT_UNITS, 
        INVALID_PATIENT_NON_NUMERIC_WEIGHT
    )
    
    result = run(df, feed_type="gp")

    # Should return empty DataFrame with correct structure
    assert len(result) == 0
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]


def test_output_dataframe_structure():
    """Test that output DataFrame has correct structure and data types"""
    df = create_single_patient_df(VALID_PATIENT_JOHN)
    
    result = run(df, feed_type="gp")

    # Verify structure
    expected_columns_flat = [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]
    
    assert list(result.columns) == expected_columns_flat
    assert len(result) == 1
    
    # Verify data preservation
    row = result.iloc[0]
    assert row['nhs_number'] == '1112223333'  # Spaces removed in processing
    assert row['given_name'] == 'John'
    assert row['family_name'] == 'Doe'
    assert row['date_of_birth'] == '01-Jan-50'  # Date remains as string since conversion is commented out
    assert row['postcode'] == 'AB1 2CD'
    assert row['sex'] == 'Male'
    assert row['height_cm'] == '175'
    assert row['height_observation_time'] == '27-Jun-25'
    assert row['weight_kg'] == '75'
    assert row['weight_observation_time'] == '27-Jun-25'


# ===== SFT Feed Specific Tests =====

def test_sft_valid_complete_records_processing():
    """Test successful processing of valid SFT records"""
    df = create_single_patient_df(VALID_SFT_PATIENT_JOHN, columns=sft_expected_columns)

    result = run(df, feed_type="sft")

    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1112223333'
    assert result.iloc[0]['given_name'] == 'John'
    assert result.iloc[0]['family_name'] == 'Doe'
    assert result.iloc[0]['sex'] == 'Male'
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth',
        'postcode', 'sex'
    ]


def test_sft_empty_nhs_number_rejection():
    """Test rejection of SFT records with empty NHS number"""
    df = create_single_patient_df(INVALID_SFT_PATIENT_EMPTY_NHS, columns=sft_expected_columns)

    result = run(df, feed_type="sft")

    assert len(result) == 0  # Record should be rejected


# ===== Parametrized Tests for Both Feeds =====

@pytest.mark.parametrize("feed_type,valid_patient,columns,expected_columns", [
    ("gp", VALID_PATIENT_JOHN, expected_columns, [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth',
        'postcode', 'sex', 'height_cm', 'height_observation_time',
        'weight_kg', 'weight_observation_time'
    ]),
    ("sft", VALID_SFT_PATIENT_JOHN, sft_expected_columns, [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth',
        'postcode', 'sex'
    ]),
])
def test_valid_record_processing_parametrized(feed_type, valid_patient, columns, expected_columns):
    """Test successful processing of valid records for different feed types"""
    df = create_single_patient_df(valid_patient, columns=columns)

    result = run(df, feed_type=feed_type)

    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1112223333'
    assert result.iloc[0]['given_name'] == 'John'
    assert result.iloc[0]['family_name'] == 'Doe'
    assert list(result.columns) == expected_columns


@pytest.mark.parametrize("feed_type,invalid_patient,columns", [
    ("gp", INVALID_PATIENT_EMPTY_NHS, expected_columns),
    ("sft", INVALID_SFT_PATIENT_EMPTY_NHS, sft_expected_columns),
])
def test_empty_nhs_rejection_parametrized(feed_type, invalid_patient, columns):
    """Test rejection of records with empty NHS number for different feed types"""
    df = create_single_patient_df(invalid_patient, columns=columns)

    result = run(df, feed_type=feed_type)

    assert len(result) == 0


@pytest.mark.parametrize("feed_type,columns", [
    ("gp", expected_columns),
    ("sft", sft_expected_columns),
])
def test_empty_dataframe_handling_parametrized(feed_type, columns):
    """Test handling of empty DataFrame for different feed types"""
    df = pd.DataFrame(columns=columns)

    result = run(df, feed_type=feed_type)

    assert len(result) == 0
    assert isinstance(result, pd.DataFrame)


@pytest.mark.parametrize("feed_type,valid_patient1,valid_patient2,columns", [
    ("gp", VALID_PATIENT_JOHN, VALID_PATIENT_JANE, expected_columns),
    ("sft", VALID_SFT_PATIENT_JOHN, VALID_SFT_PATIENT_JANE, sft_expected_columns),
])
def test_multiple_valid_records_parametrized(feed_type, valid_patient1, valid_patient2, columns):
    """Test processing of multiple valid records for different feed types"""
    df = create_test_dataframe(valid_patient1, valid_patient2, columns=columns)

    result = run(df, feed_type=feed_type)

    assert len(result) == 2
    assert result.iloc[0]['given_name'] == 'John'
    assert result.iloc[1]['given_name'] == 'Jane'
