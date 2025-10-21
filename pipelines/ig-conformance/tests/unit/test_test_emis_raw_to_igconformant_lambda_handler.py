import pytest
import pandas as pd
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from aws.pipeline.emis.raw_to_igconformant import handler as handler_module
from aws.pipeline.emis.raw_to_igconformant.handler import lambda_handler

# Fixtures

@pytest.fixture
def sample_event():
    """Basic event structure for Lambda handler."""
    return {
        'Records': [
            {
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {'key': 'input/gp_records.csv'}
                }
            }
        ]
    }

@pytest.fixture
def sample_context():
    """Mock Lambda context."""
    context = MagicMock()
    context.function_name = "test-gp-processor"
    context.aws_request_id = "test-request-123"
    return context

@pytest.fixture
def valid_gp_records_path():
    """Path to valid GP records fixture."""
    return Path(__file__).parent.parent / 'fixtures' / 'gp_data' / 'valid_gp_records.csv'

@pytest.fixture
def empty_gp_records_path():
    """Path to empty GP records fixture."""
    return Path(__file__).parent.parent / 'fixtures' / 'gp_data' / 'empty_gp_records.csv'

@pytest.fixture
def malformed_gp_records_path():
    """Path to malformed GP records fixture."""
    return Path(__file__).parent.parent / 'fixtures' / 'gp_data' / 'malformed_gp_data.csv'

@pytest.fixture
def missing_nhs_column_path():
    """Path to GP records missing NHS number column."""
    return Path(__file__).parent.parent / 'fixtures' / 'gp_data' / 'missing_nhs_column.csv'

@pytest.fixture
def missing_nhs_numbers_path():
    """Path to GP records with missing NHS numbers."""
    return Path(__file__).parent.parent / 'fixtures' / 'gp_data' / 'missing_nhs_numbers.csv'


# Environment Variable Tests

def test_missing_cohort_store_env_var(sample_event, sample_context):
    """Test handler fails when COHORT_STORE environment variable is missing."""
    with patch.dict(os.environ, {}, clear=True):
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 400
        assert 'Missing one or more of the required environment variables' in response['body']

def test_missing_input_location_env_var(sample_event, sample_context):
    """Test handler fails when INPUT_LOCATION environment variable is missing."""
    with patch.dict(os.environ, {'COHORT_STORE': 's3://bucket/cohort.csv'}, clear=True):
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 400
        assert 'Missing one or more of the required environment variables' in response['body']

def test_missing_output_location_env_var(sample_event, sample_context):
    """Test handler fails when OUTPUT_LOCATION environment variable is missing."""
    with patch.dict(os.environ, {
        'COHORT_STORE': 's3://bucket/cohort.csv',
        'INPUT_LOCATION': 's3://bucket/gp_records.csv'
    }, clear=True):
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 400
        assert 'Missing one or more of the required environment variables' in response['body']

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_valid_environment_variables(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test handler starts processing with valid environment variables."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        mock_gp_dataframe = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Test'}])
        mock_read_csv.return_value = mock_gp_dataframe
        mock_pipeline.return_value = []
        
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 200
        assert 'GP pipeline executed successfully' in response['body']


# File I/O Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/test_output.csv'
})
def test_lambda_handler_with_valid_gp_records(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test lambda handler reading valid GP records CSV file."""
    output_file = tmp_path / 'valid_output.csv'
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('builtins.open', create=True) as mock_open, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        # Setup mocks to simulate reading the valid GP records
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        mock_gp_data = pd.DataFrame([
            {'nhs_number': '1234567890', 'name': 'Alice Johnson'},
            {'nhs_number': '9876543210', 'name': 'Bob Smith'},
            {'nhs_number': '5555555555', 'name': 'Non Member'},
            {'nhs_number': '7777777777', 'name': 'Another Member'},
            {'nhs_number': '8888888888', 'name': 'Third Member'}
        ])
        mock_read_csv.return_value = mock_gp_data
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing
        assert response['statusCode'] == 200
        assert response['records_processed'] == 5
        # Should process records and filter based on cohort membership

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/empty_records.csv',
    'OUTPUT_LOCATION': '/tmp/empty_output.csv'
})
def test_lambda_handler_with_empty_gp_records(sample_event, sample_context, empty_gp_records_path):
    """Test lambda handler reading empty GP records file."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup mocks to simulate empty GP records
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_read_csv.return_value = pd.DataFrame()  # Empty DataFrame
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing of empty data
        assert response['statusCode'] == 200
        assert response['records_processed'] == 0

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/nonexistent.csv',
    'OUTPUT_LOCATION': '/tmp/error_output.csv'
})
def test_lambda_handler_with_nonexistent_gp_file(sample_event, sample_context):
    """Test lambda handler with non-existent GP records file."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup mocks to simulate file not found
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_read_csv.side_effect = FileNotFoundError("No such file or directory")
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify error handling
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'No such file or directory' in response_body['message']

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/malformed.csv',
    'OUTPUT_LOCATION': '/tmp/malformed_output.csv'
})
def test_lambda_handler_with_malformed_gp_records(sample_event, sample_context, malformed_gp_records_path):
    """Test lambda handler reading malformed CSV file."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup mocks to simulate malformed but readable CSV
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_malformed_data = pd.DataFrame([
            {'invalid': 'data', 'format': 'here', 'here': 'not_nhs_number'},
            {'invalid': 'malformed_data', 'format': 'Another Name', 'here': None}
        ])
        mock_read_csv.return_value = mock_malformed_data
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully even with malformed structure
        assert response['statusCode'] == 200
        assert response['records_processed'] == 2


# Pipeline Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_pipeline_filters_cohort_members(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test pipeline correctly filters cohort members."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        # Mock reading cohort and GP records
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        gp_dataframe = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12', 'ethnicity': 'White', 'postcode': 'TA1 1AA'}])
        mock_read_csv.return_value = gp_dataframe
        
        # Mock pipeline returning filtered records
        expected_filtered = [{'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12', 'ethnicity': 'White', 'postcode': 'TA1 1AA'}]
        mock_pipeline.return_value = expected_filtered
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        assert response['records_processed'] == 1
        assert response['records_filtered'] == 1
        # Verify pipeline was called with converted data
        mock_pipeline.assert_called_once()

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_pipeline_no_cohort_matches(sample_event, sample_context, valid_gp_records_path):
    """Test pipeline when no records match cohort."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        gp_dataframe = pd.DataFrame([{'nhs_number': '5555555555', 'name': 'Non Member'}])
        mock_read_csv.return_value = gp_dataframe
        mock_pipeline.return_value = []  # No matches
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        assert response['records_processed'] == 1
        assert response['records_filtered'] == 0


# Error Handling Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_file_read_error_handling(sample_event, sample_context):
    """Test handler handles file reading errors gracefully."""
    with patch('pandas.read_csv') as mock_read_csv:
        mock_read_csv.side_effect = FileNotFoundError("Input file not found")
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Input file not found' in response_body['message']

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_pipeline_error_handling(sample_event, sample_context):
    """Test handler handles pipeline errors gracefully."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_gp_dataframe = pd.DataFrame([{'nhs_number': '1234567890'}])
        mock_read_csv.return_value = mock_gp_dataframe
        mock_pipeline.side_effect = Exception("Pipeline processing error")
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Pipeline processing error' in response_body['message']

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/filtered_gp_records.csv'
})
def test_file_write_error_handling(sample_event, sample_context):
    """Test handler handles file writing errors gracefully."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_gp_dataframe = pd.DataFrame([{'nhs_number': '1234567890'}])
        mock_read_csv.return_value = mock_gp_dataframe
        mock_pipeline.return_value = [{'nhs_number': '1234567890'}]
        mock_to_csv.side_effect = OSError("Cannot write output file")
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Cannot write output file' in response_body['message']


# Data Validation Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/missing_nhs_column.csv',
    'OUTPUT_LOCATION': '/tmp/missing_nhs_output.csv'
})
def test_lambda_handler_with_missing_nhs_column(sample_event, sample_context, missing_nhs_column_path):
    """Test lambda handler with GP records file missing NHS number column."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup mocks to simulate data missing NHS column
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_missing_nhs_data = pd.DataFrame([
            {'name': 'John Smith', 'dob': '1980-01-15', 'ethnicity': 'White', 'postcode': 'TA1 1AA'},
            {'name': 'Jane Doe', 'dob': '1975-06-22', 'ethnicity': 'Asian', 'postcode': 'BS1 2BB'}
        ])
        mock_read_csv.return_value = mock_missing_nhs_data
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully but won't find cohort members
        assert response['statusCode'] == 200
        assert response['records_processed'] == 2
        assert response['records_filtered'] == 0  # No NHS column means no matches

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/missing_nhs_values.csv',
    'OUTPUT_LOCATION': '/tmp/missing_nhs_values_output.csv'
})
def test_lambda_handler_with_missing_nhs_numbers(sample_event, sample_context, missing_nhs_numbers_path):
    """Test lambda handler with GP records containing some missing NHS numbers."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup mocks to simulate data with some missing NHS numbers
        mock_read_cohort.return_value = pd.Series(['2345678901'])
        mock_partial_nhs_data = pd.DataFrame([
            {'nhs_number': None, 'name': 'John Smith', 'dob': '1980-01-15', 'ethnicity': 'White', 'postcode': 'TA1 1AA'},
            {'nhs_number': '2345678901', 'name': 'Jane Doe', 'dob': '1975-06-22', 'ethnicity': 'Asian', 'postcode': 'BS1 2BB'},
            {'nhs_number': '', 'name': 'Bob Johnson', 'dob': '1990-03-10', 'ethnicity': 'Black', 'postcode': 'BA1 3CC'}
        ])
        mock_read_csv.return_value = mock_partial_nhs_data
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should process successfully and find the one valid cohort member
        assert response['statusCode'] == 200
        assert response['records_processed'] == 3
        assert response['records_filtered'] == 1  # Only Jane Doe has valid NHS and is in cohort


# Event Structure Tests

def test_invalid_event_structure(sample_context):
    """Test handler with invalid event structure."""
    invalid_event = {'invalid': 'structure'}
    
    # Since the handler doesn't actually use the event structure, 
    # it should still fail due to missing env vars
    with patch.dict(os.environ, {}, clear=True):
        response = lambda_handler(invalid_event, sample_context)
        
        assert response['statusCode'] == 400
        response_body = json.loads(response['body'])
        assert 'Missing one or more of the required environment variables' in response_body['message']

def test_missing_s3_records(sample_context):
    """Test handler with missing S3 records in event."""
    event_no_records = {}
    
    # Since the handler doesn't actually use the event structure, 
    # it should still fail due to missing env vars
    with patch.dict(os.environ, {}, clear=True):
        response = lambda_handler(event_no_records, sample_context)
        
        assert response['statusCode'] == 400
        response_body = json.loads(response['body'])
        assert 'Missing one or more of the required environment variables' in response_body['message']


# Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/test_output.csv'
})
def test_end_to_end_processing_success(sample_event, sample_context, tmp_path):
    """Test complete end-to-end processing flow."""
    output_file = tmp_path / 'test_output.csv'
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        # Setup test data
        cohort_data = pd.Series(['1234567890', '9876543210'])
        gp_dataframe = pd.DataFrame([
            {'nhs_number': '1234567890', 'name': 'Alice Johnson'},
            {'nhs_number': '9876543210', 'name': 'Bob Smith'},
            {'nhs_number': '5555555555', 'name': 'Non Member'}
        ])
        filtered_records = [
            {'nhs_number': '1234567890', 'name': 'Alice Johnson'},
            {'nhs_number': '9876543210', 'name': 'Bob Smith'}
        ]  # First two are cohort members
        
        mock_read_cohort.return_value = cohort_data
        mock_read_csv.return_value = gp_dataframe
        mock_pipeline.return_value = filtered_records
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify success response
        assert response['statusCode'] == 200
        assert response['records_processed'] == 3
        assert response['records_filtered'] == 2
        assert response['output_location'] == '/tmp/test_output.csv'
        
        # Verify the to_csv method was called (output file writing)
        mock_to_csv.assert_called_once()

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/test_file_output.csv'
})
def test_output_file_creation_integration(sample_event, sample_context, tmp_path):
    """Test that output file is actually created with correct content."""
    output_file = tmp_path / 'actual_output.csv'
    
    with patch.dict(os.environ, {'OUTPUT_LOCATION': str(output_file)}), \
         patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup test data - only return cohort member in GP data
        cohort_data = pd.Series(['1234567890'])
        gp_dataframe = pd.DataFrame([
            {'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12'}
        ])
        
        mock_read_cohort.return_value = cohort_data
        mock_read_csv.return_value = gp_dataframe
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing
        assert response['statusCode'] == 200
        assert response['records_processed'] == 1
        assert response['records_filtered'] == 1
        
        # Verify output file was actually created and has correct content
        assert output_file.exists()
        result_df = pd.read_csv(output_file)
        assert len(result_df) == 1
        assert result_df.iloc[0]['nhs_number'] == '1234567890'
        assert result_df.iloc[0]['name'] == 'Alice Johnson'

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/empty_output.csv'
})
def test_end_to_end_no_cohort_members(sample_event, sample_context):
    """Test end-to-end processing when no cohort members found."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'run') as mock_pipeline:
        
        cohort_data = pd.Series(['1234567890'])
        gp_dataframe = pd.DataFrame([{'nhs_number': '5555555555', 'name': 'Non Member'}])
        
        mock_read_cohort.return_value = cohort_data
        mock_read_csv.return_value = gp_dataframe
        mock_pipeline.return_value = []  # No cohort members
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        assert response['records_processed'] == 1
        assert response['records_filtered'] == 0

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/test_cohort_read_error.csv'
})
def test_cohort_read_error_handling(sample_event, sample_context):
    """Test error handling when cohort reading fails."""
    with patch('pandas.read_csv') as mock_read_csv, \
         patch.object(handler_module, 'read_cohort_members') as mock_read_cohort:
        
        # Mock GP records reading to succeed
        gp_dataframe = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Test Patient'}])
        mock_read_csv.return_value = gp_dataframe
        
        # Setup cohort reading to raise an exception
        mock_read_cohort.side_effect = Exception("Cohort file not accessible")
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify error is properly handled
        assert response['statusCode'] == 500
        assert 'Cohort file not accessible' in response['body']

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp/test_cohort_lookup_logic.csv'
})
def test_cohort_membership_lookup_logic(sample_event, sample_context):
    """Test the cohort membership lookup logic with mixed data."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('pandas.read_csv') as mock_read_csv:
        
        # Setup test data with multiple scenarios
        cohort_data = pd.Series(['1111111111', '2222222222', '3333333333'])
        gp_dataframe = pd.DataFrame([
            {'nhs_number': '1111111111', 'name': 'Alice Johnson', 'dob': '1985-03-12'},  # Match
            {'nhs_number': '4444444444', 'name': 'Bob Smith', 'dob': '1990-01-01'},      # No match
            {'nhs_number': '2222222222', 'name': 'Carol White', 'dob': '1975-06-30'},   # Match
            {'nhs_number': '5555555555', 'name': 'David Brown', 'dob': '1980-12-15'}    # No match
        ])
        
        mock_read_cohort.return_value = cohort_data
        mock_read_csv.return_value = gp_dataframe
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify processing correctly identifies cohort members
        assert response['statusCode'] == 200
        assert response['records_processed'] == 4
        assert response['records_filtered'] == 2  # Only 2 matches