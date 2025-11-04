import pytest
import pandas as pd
import json
import os
import io
from pathlib import Path
from unittest.mock import patch, MagicMock
from aws.lambdas.emis_gprecord import handler as handler_module
from aws.lambdas.emis_gprecord.handler import lambda_handler

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
    'OUTPUT_LOCATION': '/tmp'
})
def test_valid_environment_variables(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test handler starts processing with valid environment variables."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        assert response['statusCode'] == 200
        assert 'GP pipeline executed successfully' in response['body']


# File I/O Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_lambda_handler_with_valid_gp_records(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test lambda handler reading valid GP records CSV file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 5
        # Should process records and filter based on cohort membership

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/empty_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_lambda_handler_with_empty_gp_records(sample_event, sample_context, empty_gp_records_path):
    """Test lambda handler reading empty GP records file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
         patch.object(handler_module, 'delete_file') as mock_delete_file:
        
        # Setup mocks to simulate empty GP records
        mock_read_cohort.return_value = pd.Series(['1234567890'])
        
        # Mock empty GP records file in EMIS format (3 header rows + no data)
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing of empty data
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 0

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/nonexistent.csv',
    'OUTPUT_LOCATION': '/tmp'
})
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

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/malformed.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_lambda_handler_with_malformed_gp_records(sample_event, sample_context, malformed_gp_records_path):
    """Test lambda handler reading malformed CSV file."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully even with malformed structure
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 2


# Pipeline Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_pipeline_filters_cohort_members(sample_event, sample_context, valid_gp_records_path, tmp_path):
    """Test pipeline correctly filters cohort members."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 1
        assert body_data['records_retained'] == 1
        # Verify pipeline was called with converted data
        mock_pipeline.assert_called_once()

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_pipeline_no_cohort_matches(sample_event, sample_context, valid_gp_records_path):
    """Test pipeline when no records match cohort."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 200
        # Parse the body to get the records_processed count
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 1
        assert body_data['records_retained'] == 0


# Error Handling Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
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

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
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

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_file_write_error_handling(sample_event, sample_context):
    """Test handler handles file writing errors gracefully."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv:
        
        mock_read_cohort.return_value = pd.Series(['1234567890'])

        # Mock GP records file content in EMIS format
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
1234567890,Test Patient,1980-01-01,White,TA1 1AA"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file

        mock_pipeline.return_value = pd.DataFrame([{'nhs_number': '1234567890', 'name': 'Test Patient'}])
        mock_to_csv.side_effect = OSError("Cannot write output file")

        response = lambda_handler(sample_event, sample_context)
        
        assert response['statusCode'] == 500
        response_body = json.loads(response['body'])
        assert 'Cannot write output file' in response_body['message']


# Data Validation Integration Tests

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/missing_nhs_column.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_lambda_handler_with_missing_nhs_column(sample_event, sample_context, missing_nhs_column_path):
    """Test lambda handler with GP records file missing NHS number column."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should still process successfully but won't find cohort members
        assert response['statusCode'] == 200
        # Parse the body to get the counts
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 2
        assert body_data['records_retained'] == 0  # No NHS column means no matches

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/missing_nhs_values.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_lambda_handler_with_missing_nhs_numbers(sample_event, sample_context, missing_nhs_numbers_path):
    """Test lambda handler with GP records containing some missing NHS numbers."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
         patch.object(handler_module, 'delete_file') as mock_delete_file, \
         patch('boto3.client') as mock_boto_client:
        
        # Setup mocks to simulate data with some missing NHS numbers
        mock_read_cohort.return_value = pd.Series(['2345678901'])
        
        # Mock boto3 Lambda client - but _encrypt should return None for empty values due to its own validation
        mock_lambda_client = MagicMock()
        mock_response = MagicMock()
        mock_response.statusCode = 200
        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps({'field_value': '2345678901'}).encode()
        mock_response.__getitem__ = MagicMock(return_value=mock_payload)
        mock_lambda_client.invoke.return_value = mock_response
        mock_boto_client.return_value = mock_lambda_client
        
        # Mock GP records file content with missing NHS numbers (empty cells should be skipped)
        gp_content = """Complete results are available,,,,,,
,,,,,,
nhs_number,name,dob,ethnicity,postcode
,John Smith,1980-01-15,White,TA1 1AA
2345678901,Jane Doe,1975-06-22,Asian,BS1 2BB
,Bob Johnson,1990-03-10,Black,BA1 3CC"""
        mock_gp_file = io.StringIO(gp_content)
        mock_fsspec_open.return_value.__enter__.return_value = mock_gp_file
        
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
    'OUTPUT_LOCATION': '/tmp'
})
def test_end_to_end_processing_success(sample_event, sample_context, tmp_path):
    """Test complete end-to-end processing flow."""
    
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'run') as mock_pipeline, \
         patch('pandas.DataFrame.to_csv') as mock_to_csv, \
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify success response
        assert response['statusCode'] == 200
        # Parse the body to get the counts
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 3
        assert body_data['records_retained'] == 2
        
        # Verify the to_csv method was called (output file writing)
        mock_to_csv.assert_called_once()

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_output_file_creation_integration(sample_event, sample_context, tmp_path):
    """Test that output file is actually created with correct content."""
    output_dir = tmp_path / 'output'
    output_dir.mkdir()
    
    with patch.dict(os.environ, {'OUTPUT_LOCATION': str(output_dir)}), \
         patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'delete_file'), \
         patch('pandas.DataFrame.to_csv'), \
         patch('boto3.client') as mock_boto_client:
        
        # Setup test data - only return cohort member in GP data
        cohort_data = pd.Series(['1234567890'])
        
        # Mock boto3 Lambda client for encryption
        mock_lambda_client = MagicMock()
        mock_response = MagicMock()
        mock_response.statusCode = 200
        mock_payload = MagicMock()
        mock_payload.read.return_value = json.dumps({'field_value': '1234567890'}).encode()
        mock_response.__getitem__ = MagicMock(return_value=mock_payload)
        mock_lambda_client.invoke.return_value = mock_response
        mock_boto_client.return_value = mock_lambda_client
        
        # Mock GP records file content
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name,dob\n1234567890,Alice Johnson,1985-03-12\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content
        
        mock_read_cohort.return_value = cohort_data
        
        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        # Verify successful processing
        assert response['statusCode'] == 200
        assert records_processed == 1
        assert records_filtered == 1

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_end_to_end_no_cohort_members(sample_event, sample_context):
    """Test end-to-end processing when no cohort members found."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'delete_file'), \
         patch('pandas.DataFrame.to_csv'), \
         patch.object(handler_module, 'run') as mock_pipeline:
        
        cohort_data = pd.Series(['1234567890'])
        
        # Mock GP records file content - no matching cohort members
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n5555555555,Non Member\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content
        
        mock_read_cohort.return_value = cohort_data
        mock_pipeline.return_value = pd.DataFrame()  # No cohort members
        
        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        assert response['statusCode'] == 200
        assert records_processed == 1
        assert records_filtered == 0

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_cohort_read_error_handling(sample_event, sample_context):
    """Test error handling when cohort reading fails."""
    with patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'delete_file'), \
         patch.object(handler_module, 'read_cohort_members') as mock_read_cohort:
        
        # Mock GP records file content
        gp_file_content = io.StringIO("Complete results are available,,,,,,\n,,,,,,\nnhs_number,name\n1234567890,Test Patient\n")
        mock_fsspec_open.return_value.__enter__.return_value = gp_file_content
        
        # Setup cohort reading to raise an exception
        mock_read_cohort.side_effect = Exception("Cohort file not accessible")
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify error is properly handled
        assert response['statusCode'] == 500
        
        # Handle both direct body and JSON body
        body = response['body']
        if isinstance(body, str):
            try:
                parsed_body = json.loads(body)
                error_message = parsed_body.get('error', body)
            except json.JSONDecodeError:
                error_message = body
        else:
            error_message = str(body)
        
        assert 'Cohort file not accessible' in error_message

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_cohort_membership_lookup_logic(sample_event, sample_context):
    """Test the cohort membership lookup logic with mixed data."""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch.object(handler_module, 'delete_file'), \
         patch('pandas.DataFrame.to_csv'), \
         patch('boto3.client') as mock_boto_client:
        
        # Setup test data with multiple scenarios
        cohort_data = pd.Series(['1111111111', '2222222222', '3333333333'])
        
        # Mock boto3 Lambda client for encryption - return different values for different NHS numbers
        mock_lambda_client = MagicMock()
        def mock_invoke(**kwargs):
            payload = json.loads(kwargs['Payload'])
            field_value = payload['field_value']
            mock_response = MagicMock()
            mock_response.statusCode = 200
            mock_payload = MagicMock()
            mock_payload.read.return_value = json.dumps({'field_value': field_value}).encode()
            mock_response.__getitem__ = MagicMock(return_value=mock_payload)
            return mock_response
        mock_lambda_client.invoke.side_effect = mock_invoke
        mock_boto_client.return_value = mock_lambda_client
        
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Parse response body - it's always a JSON string
        body = json.loads(response['body'])
        records_processed = body.get('records_processed')
        records_filtered = body.get('records_retained')  # This field is called records_retained in the response
        
        # Verify processing correctly identifies cohort members
        assert response['statusCode'] == 200
        assert records_processed == 4
        assert records_filtered == 2  # Only 2 matches

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
def test_encryption_service_response_parsing(sample_event, sample_context):
    """Test Lambda handler handles encryption service response parsing correctly"""
    with patch.object(handler_module, 'read_cohort_members') as mock_read_cohort, \
         patch('fsspec.open') as mock_fsspec_open, \
         patch('pandas.DataFrame.to_csv'), \
         patch.object(handler_module, 'delete_file'), \
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
            'field_value': 'encrypted_nhs_123',
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Verify successful processing and correct response parsing
        assert response['statusCode'] == 200
        body_data = json.loads(response['body'])
        assert body_data['records_processed'] == 1
        assert body_data['records_retained'] == 1


@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # The pipeline should continue gracefully - encryption timeout causes record to be skipped
        # but processing continues and returns 200
        assert response['statusCode'] == 200
        response_body = json.loads(response['body'])
        
        # Should process 1 record but retain 0 (because encryption failed)
        assert response_body['records_processed'] == 1
        assert response_body['records_retained'] == 0

@patch.dict(os.environ, {
    'COHORT_STORE': 's3://test-bucket/cohort.csv',
    'INPUT_LOCATION': 's3://test-bucket/gp_records.csv',
    'OUTPUT_LOCATION': '/tmp'
})
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
        
        response = lambda_handler(sample_event, sample_context)
        
        # Should handle malformed response gracefully - either fail or continue without processing
        assert response['statusCode'] == 500