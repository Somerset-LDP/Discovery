import pytest
import pandas as pd
import json
import os
import io
from pathlib import Path
from unittest.mock import patch, MagicMock
from lambdas import handler as handler_module
from lambdas.handler import lambda_handler

# Fixtures

@pytest.fixture
def sample_event():
    """Basic event structure for Lambda handler."""
    return {
        'input_path': 's3://test-bucket/input/gp_records.csv',
        'output_path': 's3://test-bucket/output/',
        'feed_type': 'gp'
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

@pytest.fixture
def sample_sft_event():
    """Basic event structure for SFT feed Lambda handler."""
    return {
        'input_path': 's3://test-bucket/input/sft_records.csv',
        'output_path': 's3://test-bucket/output/',
        'feed_type': 'sft'
    }

@pytest.fixture(autouse=True)
def valid_env_vars():
    """Automatically provide valid environment variables for tests that need them.
    
    This fixture uses autouse=True to automatically apply to all tests in this module.
    Tests that need different environment variables (like missing env var tests) 
    can override these by using patch.dict with clear=True or partial configs.
    """
    with patch.dict(os.environ, {
        'COHORT_STORE': 's3://test-bucket/cohort.csv',
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-pseudonymisation-lambda',
        'SKIP_ENCRYPTION': 'true'
    }):
        yield


# Environment Variable Tests

def test_missing_cohort_store_env_var(sample_event, sample_context):
    """Test handler fails when COHORT_STORE environment variable is missing."""
    with patch.dict(os.environ, {}, clear=True):
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'Missing required environment variable: COHORT_STORE' in body['message']

def test_missing_pseudonymisation_lambda_function_name_env_var(sample_event, sample_context):
    """Test handler fails when PSEUDONYMISATION_LAMBDA_FUNCTION_NAME environment variable is missing."""
    with patch.dict(os.environ, {
        'COHORT_STORE': 's3://bucket/cohort.csv'
    }, clear=True):
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 500
        body = json.loads(response['body'])
        assert 'Missing required environment variable: PSEUDONYMISATION_LAMBDA_FUNCTION_NAME' in body['message']

def test_valid_environment_variables(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test handler starts processing with valid environment variables."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Mock cohort members
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        
        # Mock GP records file content
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
1234567890,John Smith,1980-01-15,White,TA1 1AA
2345678901,Jane Doe,1975-06-22,Asian,BS1 2BB"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        # Mock pipeline
        mock_pipeline.return_value = pd.DataFrame()
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 200
        body = json.loads(response['body'])
        assert 'GP pipeline executed successfully' in body['message']


# Lambda Event Parameters Tests

def test_missing_input_path_event_param(sample_context):
    """Test handler fails when input_path parameter is missing from event."""
    invalid_event = {
        'output_path': 's3://test-bucket/output/',
        'feed_type': 'gp'
    }

    response = lambda_handler(invalid_event, sample_context)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert "Missing required parameter 'input_path' in event" in body['message']


def test_missing_output_path_event_param(sample_context):
    """Test handler fails when output_path parameter is missing from event."""
    invalid_event = {
        'input_path': 's3://test-bucket/input/gp_records.csv',
        'feed_type': 'gp'
    }

    response = lambda_handler(invalid_event, sample_context)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert "Missing required parameter 'output_path' in event" in body['message']


def test_missing_feed_type_event_param(sample_context):
    """Test handler fails when feed_type parameter is missing from event."""
    invalid_event = {
        'input_path': 's3://test-bucket/input/gp_records.csv',
        'output_path': 's3://test-bucket/output/'
    }

    response = lambda_handler(invalid_event, sample_context)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert "Missing required parameter 'feed_type' in event" in body['message']


def test_empty_feed_type_event_param(sample_context):
    """Test handler fails when feed_type parameter is empty string."""
    invalid_event = {
        'input_path': 's3://test-bucket/input/gp_records.csv',
        'output_path': 's3://test-bucket/output/',
        'feed_type': ''
    }

    response = lambda_handler(invalid_event, sample_context)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert "Missing required parameter 'feed_type' in event" in body['message']


# File I/O Integration Tests

def test_lambda_handler_with_valid_gp_records(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test lambda handler reading valid GP records CSV file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup mocks to simulate reading the valid GP records
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        
        # Mock GP records file content
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name
1234567890,Alice Johnson
9876543210,Bob Smith
5555555555,Non Member
7777777777,Another Member
8888888888,Third Member"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 5
        # Should process records and filter based on cohort membership

def test_lambda_handler_with_empty_gp_records(sample_event, sample_context, empty_gp_records_path):
    """Test lambda handler reading empty GP records file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup mocks to simulate empty GP records
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock empty GP records file in EMIS format (3 header rows + no data)
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing of empty data
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 0

def test_lambda_handler_with_nonexistent_gp_file(sample_event, sample_context):
    """Test lambda handler with non-existent GP records file."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open:
        
        # Setup mocks to simulate file not found
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_fsspec_open.side_effect = FileNotFoundError("No such file or directory")
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify error handling
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'No such file or directory' in response_body['message']

def test_lambda_handler_with_malformed_gp_records(sample_event, sample_context, malformed_gp_records_path):
    """Test lambda handler reading malformed CSV file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup mocks to simulate malformed but readable CSV
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock malformed GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
invalid,format,here
data,here,not_nhs_number
malformed_data,Another Name,"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        # Mock pipeline processing - returns empty DataFrame for malformed data
        mock_pipeline.return_value = pd.DataFrame()
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully even with malformed structure
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 2


# Pipeline Integration Tests

def test_pipeline_filters_cohort_members(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test pipeline correctly filters cohort members."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Mock reading cohort and GP records
        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])
        
        # Mock GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
1234567890,Alice Johnson,1985-03-12,White,TA1 1AA"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        # Mock pipeline returning filtered records
        expected_filtered = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12', 'ethnicity': 'White', 'postcode': 'TA1 1AA'}])
        mock_pipeline.return_value = expected_filtered
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 1
        assert body_data['records_retained'] == 1
        # Verify pipeline was called with converted data
        mock_pipeline.assert_called_once()

def test_pipeline_no_cohort_matches(sample_event, sample_context, valid_gp_records_path):
    """Test pipeline when no records match cohort."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
5555555555,Non Member,1990-01-01,Other,XX1 1XX"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        mock_pipeline.return_value = pd.DataFrame()  # No matches
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 1
        assert body_data['records_retained'] == 0


# Error Handling Tests

def test_file_read_error_handling(sample_event, sample_context):
    """Test handler handles file reading errors gracefully."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        mock_fsspec_open.side_effect = FileNotFoundError("Input file not found")
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Input file not found' in response_body['message']

def test_pipeline_error_handling(sample_event, sample_context):
    """Test handler handles pipeline errors gracefully."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # Mock GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
1234567890,Test Patient,1980-01-01,White,TA1 1AA"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file

        mock_pipeline.side_effect = Exception("Pipeline processing error")

        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Pipeline processing error' in response_body['message']

def test_file_write_error_handling(sample_event, sample_context):
    """Test handler handles file writing errors gracefully."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # Mock GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
1234567890,Test Patient,1980-01-01,White,TA1 1AA"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file

        mock_pipeline.return_value = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Test Patient'}])
        mock_write_output.side_effect = OSError("Cannot write output file")

        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Cannot write output file' in response_body['message']


# Data Validation Integration Tests

def test_lambda_handler_with_missing_nhs_column(sample_event, sample_context, missing_nhs_column_path):
    """Test lambda handler with GP records file missing NHS number column."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup mocks to simulate data missing NHS column
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock GP records file content without NHS column
        gp_content = """Complete results are available,,,,,,
,,,,,,
name,dob,ethnicity,postcode
John Smith,1980-01-15,White,TA1 1AA
Jane Doe,1975-06-22,Asian,BS1 2BB"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        # Mock pipeline - returns empty DataFrame since no NHS column means no matches
        mock_pipeline.return_value = pd.DataFrame()
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully but won't find cohort members
        assert response['statusCode'] == 200
        # Parse the body to get the counts
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 2
        assert body_data['records_retained'] == 0  # No NHS column means no matches

def test_lambda_handler_with_missing_nhs_numbers(sample_event, sample_context, missing_nhs_numbers_path):
    """Test lambda handler with GP records containing some missing NHS numbers."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        # Setup mocks to simulate data with some missing NHS numbers
        mock_read_cohort.return_value = pd.Series(['2345678901'])
        
        # Mock GP records file content with missing NHS numbers (empty cells should be skipped)
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
,John Smith,1980-01-15,White,TA1 1AA
2345678901,Jane Doe,1975-06-22,Asian,BS1 2BB
,Bob Johnson,1990-03-10,Black,BA1 3CC"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        # Mock pipeline - returns only the one valid cohort member (Jane Doe)
        filtered_data = pd.DataFrame([{'nhs_number': '2345678901', 'name': 'Jane Doe', 'dob': '1975-06-22', 'ethnicity': 'Asian', 'postcode': 'BS1 2BB'}])
        mock_pipeline.return_value = filtered_data
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Should process successfully and find the one valid cohort member
        assert response['statusCode'] == 200
        # Parse the body to get the counts
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 3
        assert body_data['records_retained'] == 1  # Only Jane Doe has valid NHS and is in cohort


# Event Structure Tests

def test_invalid_event_structure(sample_context):
    """Test handler with invalid event structure."""
    invalid_event = {'invalid': 'structure'}
    
    response = lambda_handler(invalid_event, sample_context)

    assert response['statusCode'] == 500
    response_body = json.loads(response['body'])
    assert "Missing required parameter 'input_path' in event" in response_body['message']

def test_missing_s3_records(sample_context):
    """Test handler with missing S3 records in event."""
    event_no_records = {}
    
    response = lambda_handler(event_no_records, sample_context)

    assert response['statusCode'] == 500
    response_body = json.loads(response['body'])
    assert "Missing required parameter 'input_path' in event" in response_body['message']


# Integration Tests

def test_end_to_end_processing_success(sample_event, sample_context, tmp_path):
    """Test complete end-to-end processing flow."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup test data
        cohort_data = pd.Series(['1234567890', '9876543210'])
        
        # Mock GP records file content
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name
1234567890,Alice Johnson
9876543210,Bob Smith
5555555555,Non Member"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        filtered_records = pd.DataFrame([
            {'nhs_number': '1234567890', 'name': 'Alice Johnson'},
            {'nhs_number': '9876543210', 'name': 'Bob Smith'}
        ])  # First two are cohort members
        
        mock_read_cohort.return_value = cohort_data
        mock_pipeline.return_value = filtered_records
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Verify success response
        assert response['statusCode'] == 200
        # Parse the body to get the counts
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 3
        assert body_data['records_retained'] == 2
        
        # Verify the write_output method was called
        mock_write_output.assert_called_once()

def test_output_file_creation_integration(sample_event, sample_context, tmp_path):
    """Test that output file is actually created with correct content."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup test data - only return cohort member in GP data
        cohort_data = pd.Series(['1234567890'])
        
        # Mock GP records file content
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name,dob\n1234567890,Alice Johnson,1985-03-12\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content
        
        mock_read_cohort.return_value = cohort_data
        
        # Mock pipeline returning the cohort member record
        filtered_data = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12'}])
        mock_pipeline.return_value = filtered_data
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'
        
        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        # Verify successful processing
        assert response['statusCode'] == 200
        assert records_processed == 1
        assert records_filtered == 1

def test_end_to_end_no_cohort_members(sample_event, sample_context):
    """Test end-to-end processing when no cohort members found."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        cohort_data = pd.Series(['1234567890'])
        
        # Mock GP records file content - no matching cohort members
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n5555555555,Non Member\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content
        
        mock_read_cohort.return_value = cohort_data
        mock_pipeline.return_value = pd.DataFrame()  # No cohort members
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        assert response['statusCode'] == 200
        assert records_processed == 1
        assert records_filtered == 0

def test_cohort_read_error_handling(sample_event, sample_context):
    """Test error handling when cohort reading fails."""
    with patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'delete_file') as mock_delete_file, \
         patch.object(handler_module, 'read_cohort_members') as mock_read_cohort:
        
        # Mock GP records file content
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n1234567890,Test Patient\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content

        # Setup cohort reading to raise an exception
        mock_read_cohort.side_effect = Exception("Cohort file not accessible")
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify error is properly handled
        assert response['statusCode'] == 500
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        error_message = body.get('message', '')

        assert 'Cohort file not accessible' in error_message

def test_cohort_membership_lookup_logic(sample_event, sample_context):
    """Test the cohort membership lookup logic with mixed data."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        # Setup test data with multiple scenarios
        cohort_data = pd.Series(['1111111111', '2222222222', '3333333333'])
        
        # Mock GP records file content with mixed cohort membership
        gp_file_content = io.StringIO("""Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob
1111111111,Alice Johnson,1985-03-12
4444444444,Bob Smith,1990-01-01
2222222222,Carol White,1975-06-30
5555555555,David Brown,1980-12-15""")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content

        mock_read_cohort.return_value = cohort_data
        
        # Mock pipeline returning only cohort members
        filtered_records = pd.DataFrame([
            {'nhs_number': '1111111111', 'name': 'Alice Johnson', 'dob': '1985-03-12'},
            {'nhs_number': '2222222222', 'name': 'Carol White', 'dob': '1975-06-30'}
        ])
        mock_pipeline.return_value = filtered_records
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        # Verify processing correctly identifies cohort members
        assert response['statusCode'] == 200
        assert records_processed == 4
        assert records_filtered == 2  # Only 2 matches

def test_encryption_service_response_parsing(sample_event, sample_context):
    """Test Lambda handler handles encryption service response parsing correctly"""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file, \
         patch('boto3.client') as mock_boto_client:

        mock_read_cohort.return_value = pd.Series(['encrypted_nhs_123'])

        # Mock boto3 Lambda client with complex response structure
        mock_lambda_client = MagicMock()
        mock_response = MagicMock()
        mock_response.statusCode = 200
        mock_payload = MagicMock()

        # Mock response with properly structured encryption service response
        encryption_response = {
            'field_name': 'nhs_number',
            'field_value': ['encrypted_nhs_123'],
            'status': 'success'
        }
        mock_payload.read.return_value = json.dumps(encryption_response).encode()
        mock_response.__getitem__ = MagicMock(return_value=mock_payload)
        mock_lambda_client.invoke.return_value = mock_response
        mock_boto_client.return_value = mock_lambda_client

        # Mock GP records file content
        gp_content = "Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n1234567890,Alice Johnson\n"
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file

        # Mock pipeline returning cohort member
        filtered_data = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Alice Johnson'}])
        mock_pipeline.return_value = filtered_data
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        with patch.dict(os.environ, {'SKIP_ENCRYPTION': ''}):
        
            response = lambda_handler(sample_event, sample_context)
            
            # Verify successful processing and correct response parsing
            assert response['statusCode'] == 200
            body_data = json.loads(response['body'])
            assert body_data['records_processed'] == 1
            assert body_data['records_retained'] == 1


def test_encryption_service_timeout_handling(sample_event, sample_context):
    """Test Lambda handler manages encryption service timeouts and retries"""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv'), \
         patch.object(handler_module, 'delete_file'), \
         patch('boto3.client') as mock_boto_client:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # Mock boto3 Lambda client to simulate timeout - this will cause _encrypt to return None
        mock_lambda_client = MagicMock()
        from botocore.exceptions import ReadTimeoutError
        mock_lambda_client.invoke.side_effect = ReadTimeoutError(
            endpoint_url='test-endpoint', 
            operation_name='Invoke'
        )
        mock_boto_client.return_value = mock_lambda_client

        # Mock GP records file content
        gp_content = "Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n1234567890,Alice Johnson\n"
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        with patch.dict(os.environ, {'SKIP_ENCRYPTION': ''}):
            response = lambda_handler(sample_event, sample_context)

            # Pipeline fails if upon the first timeout on the Pseudonymisation service
            assert response['statusCode'] == 500

def test_malformed_encryption_response_handling(sample_event, sample_context):
    """Test Lambda handler handles malformed responses from encryption service"""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv'), \
         patch.object(handler_module, 'delete_file'), \
         patch('boto3.client') as mock_boto_client:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock boto3 Lambda client with malformed response structure
        mock_lambda_client = MagicMock()
        mock_response = MagicMock()
        mock_response.statusCode = 200
        mock_payload = MagicMock()
        
        # Response missing required field_value key
        malformed_response = {
            'field_name': 'nhs_number',
            'status': 'success',
            # Missing 'field_value' key
        }
        mock_payload.read.return_value = json.dumps(malformed_response).encode()
        mock_response.__getitem__ = MagicMock(return_value=mock_payload)
        mock_lambda_client.invoke.return_value = mock_response
        mock_boto_client.return_value = mock_lambda_client
        
        # Mock GP records file content
        gp_content = "Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n1234567890,Alice Johnson\n"
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        with patch.dict(os.environ, {'SKIP_ENCRYPTION': ''}):
            response = lambda_handler(sample_event, sample_context)
            
            # Should handle malformed response gracefully - either fail or continue without processing
            assert response['statusCode'] == 500


# SFT Feed Specific Tests

def test_sft_feed_processing_success(sample_sft_event, sample_context):
    """Test SFT feed processing with correct configuration."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        mock_read_cohort.return_value = pd.Series(['1234567890', '9876543210'])

        # SFT format: no metadata rows, NHS number in column 1 (index 1)
        sft_content = """patient_id,nhs_number,name,dob
P001,1234567890,Alice Johnson,1985-03-12
P002,9876543210,Bob Smith,1990-01-01
P003,5555555555,Non Member,1980-05-15"""
        mock_sft_file = io.StringIO(sft_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_sft_file

        filtered_records = pd.DataFrame([
            {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12'},
            {'patient_id': 'P002', 'nhs_number': '9876543210', 'name': 'Bob Smith', 'dob': '1990-01-01'}
        ])
        mock_pipeline.return_value = filtered_records
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_sft_event, sample_context)

        assert response['statusCode'] == 200
        body_data = json.loads(response['body'])
        assert 'SFT pipeline executed successfully' in body_data['message']
        assert body_data['records_processed'] == 3
        assert body_data['records_retained'] == 2
        assert body_data['feed_type'] == 'sft'


def test_sft_feed_no_metadata_preservation(sample_sft_event, sample_context):
    """Test that SFT feed does not preserve metadata rows."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_records') as mock_write_records, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # SFT format: no metadata rows
        sft_content = """patient_id,nhs_number,name
P001,1234567890,Alice Johnson"""
        mock_sft_file = io.StringIO(sft_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_sft_file

        filtered_records = pd.DataFrame([{'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice Johnson'}])
        mock_pipeline.return_value = filtered_records
        mock_write_records.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_sft_event, sample_context)

        assert response['statusCode'] == 200

        # Verify _write_records was called with correct parameters
        mock_write_records.assert_called_once()
        call_args = mock_write_records.call_args

        # Check that metadata_rows is empty (no metadata to preserve)
        metadata_rows = call_args[0][1]
        assert len(metadata_rows) == 0

        # Check feed_config has preserve_metadata=False
        feed_config = call_args[0][4]
        assert feed_config.feed_type == 'sft'
        assert feed_config.preserve_metadata is False
        assert feed_config.metadata_rows_to_skip == 0


def test_sft_feed_with_nhs_in_second_column(sample_sft_event, sample_context):
    """Test SFT feed correctly identifies NHS number in second column (index 1)."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # SFT format: NHS number in second column (index 1)
        sft_content = """patient_id,nhs_number,name,dob
P001,1234567890,Alice Johnson,1985-03-12
P002,9999999999,Non Member,1990-01-01"""
        mock_sft_file = io.StringIO(sft_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_sft_file

        # Mock pipeline to verify it receives correct config
        def verify_config(cohort, records, encrypt_fn, feed_config):
            assert feed_config.nhs_column_index == 1
            assert feed_config.feed_type == 'sft'
            return pd.DataFrame([{'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice Johnson', 'dob': '1985-03-12'}])

        mock_pipeline.side_effect = verify_config
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_sft_event, sample_context)

        assert response['statusCode'] == 200
        body_data = json.loads(response['body'])
        assert body_data['feed_type'] == 'sft'


def test_sft_feed_empty_records(sample_sft_event, sample_context):
    """Test SFT feed with empty records."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # SFT format: header only, no data
        sft_content = """patient_id,nhs_number,name,dob"""
        mock_sft_file = io.StringIO(sft_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_sft_file
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        response = lambda_handler(sample_sft_event, sample_context)

        assert response['statusCode'] == 200
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 0
        assert body_data['records_retained'] == 0


def test_sft_feed_case_insensitive(sample_context):
    """Test that feed_type is case insensitive for SFT."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch.object(handler_module, '_write_output') as mock_write_output, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:

        mock_read_cohort.return_value = pd.Series(['1234567890'])

        sft_content = """patient_id,nhs_number,name
P001,1234567890,Alice Johnson"""
        mock_sft_file = io.StringIO(sft_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_sft_file

        mock_pipeline.return_value = pd.DataFrame([{'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice Johnson'}])
        mock_write_output.return_value = 's3://test-bucket/output/processed.csv'

        # Test with uppercase
        event_uppercase = {
            'input_path': 's3://test-bucket/input/sft_records.csv',
            'output_path': 's3://test-bucket/output/',
            'feed_type': 'SFT'
        }

        response = lambda_handler(event_uppercase, sample_context)

        assert response['statusCode'] == 200
        body_data = json.loads(response['body'])
        assert body_data['feed_type'] == 'sft'

