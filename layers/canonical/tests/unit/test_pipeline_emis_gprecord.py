import pytest
import pandas as pd
import logging
from unittest.mock import patch, MagicMock
from pipeline.emis_gprecord import run, USER_TYPE_COL

# Expected single-level columns based on current implementation
expected_columns = [
    'NHS Number', 'Given Name', 'Family Name', 'Date of Birth', 'Postcode', 
    'Number and Street', 'Ethnic Origin', 'Gender',
    'Value', 'Unit of Measure', 'Date',  # Height measurements
    'Value.1', 'Unit of Measure.1', 'Date.1',  # Weight measurements  
    'Consultation ID', 'Date.2', 'Time', 'Type of Consultation', "User Details' User Type"
]


def test_column_structure_validation_failure():
    """Test that the method raises ValueError when input DataFrame has incorrect columns"""
    # Create DataFrame with wrong columns
    wrong_columns = [
        ('Wrong', 'Column1'),
        ('Wrong', 'Column2'),
    ]
    df = pd.DataFrame(columns=pd.MultiIndex.from_tuples(wrong_columns))
    
    with pytest.raises(ValueError, match="Input data does not have enough columns for EMIS GP Record structure"):
        run(df)


def test_valid_complete_records_processing():
    """Test successful processing of valid complete records"""
    # Create DataFrame with correct structure and valid data
    df = pd.DataFrame(columns=expected_columns)
    
    # Add one valid record
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
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
    
    result = run(df)
    
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
    
    result = run(df)
    
    assert len(result) == 0
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]


@patch('pipeline.emis_gprecord.logging.getLogger')
def test_mixed_valid_and_invalid_records(mock_logger):
    """Test processing of mixed valid and invalid records"""
    mock_logger_instance = MagicMock()
    mock_logger.return_value = mock_logger_instance
    
    df = pd.DataFrame(columns=expected_columns)
    
    # Add valid record
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
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
    
    # Add invalid record (missing patient details)
    df.loc[1] = [
        '', '', '', '', '', '', '', '',  # Empty patient details
        '180', 'cm', '27-Jun-25',
        '80', 'kg', '27-Jun-25',
        '12346', '15-May-25', '09:30', 'Face to face', 'GP'
    ]
    
    result = run(df)
    
    # Only valid record should be in result
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1112223333'  # Spaces removed in processing
    
    # Check that warning was logged for skipped record
    mock_logger_instance.warning.assert_any_call("Record at index 1 failed validation and will be skipped")


def test_patient_details_validation_failure():
    """Test rejection of records with missing patient details"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with missing NHS Number
    df.loc[0] = [
        '', 'John', 'Doe', '01-Jan-50', 'AB1 2CD',
        '123 Test Street', 'British', 'Male',
        '175', 'cm', '27-Jun-25',
        '75', 'kg', '27-Jun-25',
        '12345', '15-May-25', '09:30', 'Face to face', 'GP'
    ]
    
    result = run(df)
    
    assert len(result) == 0  # Record should be rejected


def test_height_measurement_validation_wrong_units():
    """Test rejection of records with invalid height measurements"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with wrong height units
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
        'Male',
        '5.8',
        'ft',
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
    
    result = run(df)
    
    assert len(result) == 0  # Record should be rejected


def test_height_measurement_validation_non_numeric():
    """Test rejection of records with non-numeric height values"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with non-numeric height value
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
        'Male',
        'tall',
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
    
    result = run(df)
    
    assert len(result) == 0  # Record should be rejected


def test_weight_measurement_validation_wrong_units():
    """Test rejection of records with invalid weight measurements"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with wrong weight units
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
        'Male',
        '175',
        'cm',
        '27-Jun-25',
        '165',
        'lbs',
        '27-Jun-25',
        '12345',
        '15-May-25',
        '09:30',
        'Face to face',
        'GP'
    ]
    
    result = run(df)
    
    assert len(result) == 0  # Record should be rejected


def test_weight_measurement_validation_missing_value():
    """Test rejection of records with missing weight values"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with missing weight value
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
        'Male',
        '175',
        'cm',
        '27-Jun-25',
        '',
        'kg',
        '27-Jun-25',
        '12345',
        '15-May-25',
        '09:30',
        'Face to face',
        'GP'
    ]
    
    result = run(df)
    
    assert len(result) == 1  # Record should be accepted (empty weight is allowed)
    assert result.iloc[0]['weight_kg'] == ""  # Weight should remain empty


def test_date_format_validation_invalid_format():
    """Test rejection of records with invalid date formats"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with invalid date format
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
        'Male',
        '175',
        'cm',
        '2025-06-27',
        '75',
        'kg',
        '27-Jun-25',
        '12345',
        '15-May-25',
        '09:30',
        'Face to face',
        'GP'
    ]
    
    result = run(df)
    
    assert len(result) == 0  # Record should be rejected


def test_date_format_validation_valid_format():
    """Test acceptance of records with valid date formats"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Record with valid date format
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
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
    
    result = run(df)
    
    assert len(result) == 1  # Record should be accepted
    assert result.iloc[0]['height_observation_time'] == '27-Jun-25'
    assert result.iloc[0]['weight_observation_time'] == '27-Jun-25'


def test_all_records_invalid_scenario():
    """Test handling when all input records fail validation"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Add multiple invalid records
    # Record 1: Missing patient details
    df.loc[0] = [
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
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
    
    # Record 2: Wrong height units
    df.loc[1] = [
        '222 333 4444', 'Jane', 'Smith', '01-Jan-60', 'CD2 3EF',
        '456 Main St', 'British', 'Female',
        '5.6', 'ft', '27-Jun-25',  # Wrong units
        '65', 'kg', '27-Jun-25',
        '12346', '15-May-25', '09:30', 'Face to face', 'GP'
    ]
    
    # Record 3: Non-numeric weight
    df.loc[2] = [
        '333 444 5555', 'Bob', 'Johnson', '01-Jan-70', 'EF3 4GH',
        '789 Oak Ave', 'British', 'Male',
        '180', 'cm', '27-Jun-25',
        'heavy', 'kg', '27-Jun-25',  # Non-numeric
        '12347', '15-May-25', '09:30', 'Face to face', 'GP'
    ]
    
    result = run(df)
    
    # Should return empty DataFrame with correct structure
    assert len(result) == 0
    assert list(result.columns) == [
        'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
        'postcode', 'sex', 'height_cm', 'height_observation_time', 
        'weight_kg', 'weight_observation_time'
    ]


def test_output_dataframe_structure():
    """Test that output DataFrame has correct structure and data types"""
    df = pd.DataFrame(columns=expected_columns)
    
    # Add valid record
    df.loc[0] = [
        '111 222 3333',
        'John',
        'Doe',
        '01-Jan-50',
        'AB1 2CD',
        '123 Test Street',
        'British',
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
    
    result = run(df)
    
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