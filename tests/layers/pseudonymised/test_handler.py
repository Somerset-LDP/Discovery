import json
from unittest.mock import patch

import pandas as pd
import pytest

from feed_config import FeedConfig
from handler import (
    read_csv_from_s3,
    normalize_nhs_numbers,
    pseudonymise,
    write_pseudonymised_data,
    generate_output_key,
    create_response,
    process_file,
    process_all_files,
    lambda_handler,
    validate_required_params
)


@pytest.fixture
def gp_feed_config():
    return FeedConfig(
        feed_type="gp",
        metadata_rows_to_skip=2,
        preserve_metadata=True,
        fields_to_pseudonymise={
            'NHS Number': 'nhs_number',
            'Given Name': 'given_name',
            'Family Name': 'family_name',
            'Date of Birth': 'date_of_birth',
            'Gender': 'gender',
            'Postcode': 'postcode'
        },
        validation_rules={
            "valid_sex_values": ['Male', 'Female', 'Indeterminate'],
            "valid_date_format": "%d-%b-%y"
        }
    )


@pytest.fixture
def sft_feed_config():
    return FeedConfig(
        feed_type="sft",
        metadata_rows_to_skip=0,
        preserve_metadata=False,
        fields_to_pseudonymise={
            'nhs_number': 'nhs_number',
            'first_name': 'first_name',
            'last_name': 'last_name',
            'date_of_birth': 'date_of_birth',
            'sex': 'sex',
            'postcode': 'postcode'
        },
        validation_rules={
            "valid_sex_values": ['1', '2', '9'],
            "valid_date_format": "%Y-%m-%d"
        }
    )


def test_validate_required_params_returns_all_required_variables():
    env_vars = {
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda',
        'KMS_KEY_ID': 'test-kms-key-id'
    }
    param_names = ['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME', 'KMS_KEY_ID']

    result = validate_required_params(env_vars, param_names, 'environment variable')

    assert result == env_vars
    assert result['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME'] == 'test-lambda'
    assert result['KMS_KEY_ID'] == 'test-kms-key-id'


def test_validate_required_params_raises_value_error_when_variables_missing():
    env_vars = {
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda',
        'KMS_KEY_ID': ''
    }
    param_names = ['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME', 'KMS_KEY_ID']

    with pytest.raises(ValueError) as exc_info:
        validate_required_params(env_vars, param_names, 'environment variable')

    assert "Missing required environment variables" in str(exc_info.value)
    assert "KMS_KEY_ID" in str(exc_info.value)


def test_validate_required_params_raises_value_error_when_all_variables_missing():
    env_vars = {}
    param_names = ['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME', 'KMS_KEY_ID']

    with pytest.raises(ValueError) as exc_info:
        validate_required_params(env_vars, param_names, 'environment variable')

    assert "Missing required environment variables" in str(exc_info.value)
    assert "PSEUDONYMISATION_LAMBDA_FUNCTION_NAME" in str(exc_info.value)
    assert "KMS_KEY_ID" in str(exc_info.value)


def test_validate_required_params_with_event_parameters():
    event_params = {
        'input_s3_bucket': 'test-bucket',
        'input_prefix': 'test-prefix',
        'output_s3_bucket': 'output-bucket',
        'feed_type': 'gp'
    }
    param_names = ['input_s3_bucket', 'input_prefix', 'output_s3_bucket', 'feed_type']

    result = validate_required_params(event_params, param_names, 'event parameter')

    assert result == event_params
    assert result['input_s3_bucket'] == 'test-bucket'
    assert result['feed_type'] == 'gp'


def test_validate_required_params_strips_whitespace():
    params = {
        'param1': '  value1  ',
        'param2': 'value2'
    }
    param_names = ['param1', 'param2']

    result = validate_required_params(params, param_names, 'parameter')

    assert result['param1'] == 'value1'
    assert result['param2'] == 'value2'


@patch('handler.read_s3_file')
def test_read_csv_from_s3_gp_feed(mock_read_s3_file):
    csv_content = """Line 1 to skip
Line 2 to skip
NHS Number,Given Name,Family Name,Date of Birth,Gender,Postcode
9434765919,John,Doe,15-Jan-85,Male,SW1A 1AA
8314495581,Jane,Smith,20-Feb-90,Female,M1 1AA"""

    mock_read_s3_file.return_value = csv_content.encode('utf-8')

    result_df, metadata = read_csv_from_s3('test-bucket', 'test-key.csv', skiprows=2, preserve_metadata=True)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert list(result_df.columns) == ['NHS Number', 'Given Name', 'Family Name', 'Date of Birth', 'Gender', 'Postcode']
    assert result_df.iloc[0]['NHS Number'] == '9434765919'
    assert result_df.iloc[1]['Given Name'] == 'Jane'
    assert metadata == ['Line 1 to skip', 'Line 2 to skip']


@patch('handler.read_s3_file')
def test_read_csv_from_s3_sft_feed_no_metadata(mock_read_s3_file):
    csv_content = """nhs_number,first_name,last_name,date_of_birth,sex,postcode
9434765919,John,Doe,1985-01-15,Male,SW1A 1AA
8314495581,Jane,Smith,1990-02-20,Female,M1 1AA"""

    mock_read_s3_file.return_value = csv_content.encode('utf-8')

    result_df, metadata = read_csv_from_s3('test-bucket', 'test-key.csv', skiprows=0, preserve_metadata=False)

    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert list(result_df.columns) == ['nhs_number', 'first_name', 'last_name', 'date_of_birth', 'sex', 'postcode']
    assert result_df.iloc[0]['nhs_number'] == '9434765919'
    assert result_df.iloc[1]['first_name'] == 'Jane'
    assert metadata == []


@patch('handler.read_s3_file')
def test_read_csv_from_s3_raises_value_error_on_decode_failure(mock_read_s3_file):
    mock_read_s3_file.return_value = b'\x80\x81\x82\x83'

    with pytest.raises(ValueError) as exc_info:
        read_csv_from_s3('test-bucket', 'test-key.csv', skiprows=2, preserve_metadata=True)

    assert 'File encoding error' in str(exc_info.value)
    assert 'test-bucket' in str(exc_info.value)
    assert 'test-key.csv' in str(exc_info.value)


@pytest.mark.parametrize(
    "input_nhs_numbers,expected_nhs_numbers",
    [
        (['9434765919', '8314495581'], ['9434765919', '8314495581']),
        (['943 476 5919', '831 449 5581'], ['9434765919', '8314495581']),
        ([' 9434765919 ', ' 8314495581 '], ['9434765919', '8314495581']),
        (['943 476 5919 ', ' 831 449 5581'], ['9434765919', '8314495581']),
        (['9434765919', '943 476 5919', ' 9434765919 '], ['9434765919', '9434765919', '9434765919']),
    ],
)
def test_normalize_nhs_numbers_gp_feed(input_nhs_numbers, expected_nhs_numbers):
    df = pd.DataFrame({'NHS Number': input_nhs_numbers})
    fields_to_pseudonymise = {'NHS Number': 'nhs_number'}

    result = normalize_nhs_numbers(df, fields_to_pseudonymise)

    assert result['NHS Number'].tolist() == expected_nhs_numbers


def test_normalize_nhs_numbers_sft_feed():
    input_nhs_numbers = ['943 476 5919', '831 449 5581', ' 9434765919 ']
    expected_nhs_numbers = ['9434765919', '8314495581', '9434765919']

    df = pd.DataFrame({'nhs_number': input_nhs_numbers})
    fields_to_pseudonymise = {'nhs_number': 'nhs_number'}

    result = normalize_nhs_numbers(df, fields_to_pseudonymise)

    assert result['nhs_number'].tolist() == expected_nhs_numbers


@patch('handler.invoke_pseudonymisation_lambda_batch')
def test_pseudonymise_gp_feed(mock_invoke_lambda):
    df = pd.DataFrame({
        'NHS Number': ['9434765919', '8314495581'],
        'Given Name': ['John', 'Jane'],
        'Family Name': ['Doe', 'Smith'],
        'Date of Birth': ['15-Jan-85', '20-Feb-90'],
        'Gender': ['Male', 'Female'],
        'Postcode': ['SW1A 1AA', 'M1 1AA']
    })

    mock_invoke_lambda.side_effect = [
        ['pseudo_nhs_1', 'pseudo_nhs_2'],
        ['pseudo_given_1', 'pseudo_given_2'],
        ['pseudo_family_1', 'pseudo_family_2'],
        ['pseudo_dob_1', 'pseudo_dob_2'],
        ['pseudo_gender_1', 'pseudo_gender_2'],
        ['pseudo_postcode_1', 'pseudo_postcode_2']
    ]

    fields_to_pseudonymise = {
        'NHS Number': 'nhs_number',
        'Given Name': 'given_name',
        'Family Name': 'family_name',
        'Date of Birth': 'date_of_birth',
        'Gender': 'gender',
        'Postcode': 'postcode'
    }

    result = pseudonymise(df.copy(), 'test-lambda-function', fields_to_pseudonymise)

    assert len(result) == 2
    assert result.iloc[0]['NHS Number'] == 'pseudo_nhs_1'
    assert result.iloc[1]['Given Name'] == 'pseudo_given_2'
    assert mock_invoke_lambda.call_count == 6


@patch('handler.invoke_pseudonymisation_lambda_batch')
def test_pseudonymise_sft_feed(mock_invoke_lambda):
    df = pd.DataFrame({
        'nhs_number': ['9434765919', '8314495581'],
        'first_name': ['John', 'Jane'],
        'last_name': ['Doe', 'Smith'],
        'date_of_birth': ['1985-01-15', '1990-02-20'],
        'sex': ['Male', 'Female'],
        'postcode': ['SW1A 1AA', 'M1 1AA']
    })

    mock_invoke_lambda.side_effect = [
        ['pseudo_nhs_1', 'pseudo_nhs_2'],
        ['pseudo_first_1', 'pseudo_first_2'],
        ['pseudo_last_1', 'pseudo_last_2'],
        ['pseudo_dob_1', 'pseudo_dob_2'],
        ['pseudo_sex_1', 'pseudo_sex_2'],
        ['pseudo_postcode_1', 'pseudo_postcode_2']
    ]

    fields_to_pseudonymise = {
        'nhs_number': 'nhs_number',
        'first_name': 'first_name',
        'last_name': 'last_name',
        'date_of_birth': 'date_of_birth',
        'sex': 'sex',
        'postcode': 'postcode'
    }

    result = pseudonymise(df.copy(), 'test-lambda-function', fields_to_pseudonymise)

    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == 'pseudo_nhs_1'
    assert result.iloc[1]['first_name'] == 'pseudo_first_2'
    assert mock_invoke_lambda.call_count == 6


@patch('handler.invoke_pseudonymisation_lambda_batch')
def test_pseudonymise_raises_value_error_when_record_count_mismatch(mock_invoke_lambda):
    df = pd.DataFrame({
        'NHS Number': ['9434765919', '8314495581'],
        'Given Name': ['John', 'Jane'],
        'Family Name': ['Doe', 'Smith'],
        'Date of Birth': ['15-Jan-85', '20-Feb-90'],
        'Gender': ['Male', 'Female'],
        'Postcode': ['SW1A 1AA', 'M1 1AA']
    })

    mock_invoke_lambda.return_value = ['pseudo_nhs_1']

    fields_to_pseudonymise = {
        'NHS Number': 'nhs_number',
        'Given Name': 'given_name',
        'Family Name': 'family_name',
        'Date of Birth': 'date_of_birth',
        'Gender': 'gender',
        'Postcode': 'postcode'
    }

    with pytest.raises(ValueError) as exc_info:
        pseudonymise(df.copy(), 'test-lambda-function', fields_to_pseudonymise)

    assert 'Record count mismatch for field NHS Number' in str(exc_info.value)
    assert 'expected 2, got 1' in str(exc_info.value)


@patch('handler.invoke_pseudonymisation_lambda_batch')
def test_pseudonymise_raises_value_error_on_lambda_error(mock_invoke_lambda):
    df = pd.DataFrame({
        'NHS Number': ['9434765919'],
        'Given Name': ['John'],
        'Family Name': ['Doe'],
        'Date of Birth': ['15-Jan-85'],
        'Gender': ['Male'],
        'Postcode': ['SW1A 1AA']
    })

    mock_invoke_lambda.side_effect = ValueError('Lambda invocation failed')

    fields_to_pseudonymise = {
        'NHS Number': 'nhs_number',
        'Given Name': 'given_name',
        'Family Name': 'family_name',
        'Date of Birth': 'date_of_birth',
        'Gender': 'gender',
        'Postcode': 'postcode'
    }

    with pytest.raises(ValueError) as exc_info:
        pseudonymise(df.copy(), 'test-lambda-function', fields_to_pseudonymise)

    assert 'Lambda invocation failed' in str(exc_info.value)


@patch('handler.write_to_s3')
@patch('handler.generate_output_key')
def test_write_pseudonymised_data_gp_feed(mock_generate_key, mock_write_to_s3):
    df = pd.DataFrame({
        'NHS Number': ['pseudo_nhs_1', 'pseudo_nhs_2'],
        'Given Name': ['pseudo_given_1', 'pseudo_given_2'],
        'Family Name': ['pseudo_family_1', 'pseudo_family_2'],
        'Date of Birth': ['pseudo_dob_1', 'pseudo_dob_2'],
        'Gender': ['pseudo_gender_1', 'pseudo_gender_2'],
        'Postcode': ['pseudo_postcode_1', 'pseudo_postcode_2']
    })

    metadata_lines = ['Metadata line 1', 'Metadata line 2']

    mock_generate_key.return_value = 'gp_feed/2025/11/06/raw/patient_123456.csv'

    write_pseudonymised_data(df, 'test-output-bucket', 'test-kms-key', metadata_lines, preserve_metadata=True, feed_type='gp')

    mock_write_to_s3.assert_called_once()
    call_args = mock_write_to_s3.call_args
    assert call_args[0][0] == 'test-output-bucket'
    assert call_args[0][1] == 'gp_feed/2025/11/06/raw/patient_123456.csv'
    content = call_args[0][2]
    assert 'Metadata line 1\n' in content
    assert 'Metadata line 2\n' in content
    assert 'pseudo_nhs_1' in content
    assert call_args[0][3] == 'test-kms-key'
    
    # Verify generate_output_key was called with 'gp'
    mock_generate_key.assert_called_once_with('gp')


@patch('handler.write_to_s3')
@patch('handler.generate_output_key')
def test_write_pseudonymised_data_sft_feed_no_metadata(mock_generate_key, mock_write_to_s3):
    df = pd.DataFrame({
        'nhs_number': ['pseudo_nhs_1', 'pseudo_nhs_2'],
        'first_name': ['pseudo_first_1', 'pseudo_first_2'],
        'last_name': ['pseudo_last_1', 'pseudo_last_2'],
        'date_of_birth': ['pseudo_dob_1', 'pseudo_dob_2'],
        'sex': ['pseudo_sex_1', 'pseudo_sex_2'],
        'postcode': ['pseudo_postcode_1', 'pseudo_postcode_2']
    })

    metadata_lines = []

    mock_generate_key.return_value = 'sft_feed/2025/11/06/raw/patient_123456.csv'

    write_pseudonymised_data(df, 'test-output-bucket', 'test-kms-key', metadata_lines, preserve_metadata=False, feed_type='sft')

    mock_write_to_s3.assert_called_once()
    call_args = mock_write_to_s3.call_args
    assert call_args[0][0] == 'test-output-bucket'
    assert call_args[0][1] == 'sft_feed/2025/11/06/raw/patient_123456.csv'
    content = call_args[0][2]
    # Should NOT contain metadata lines for SFT
    assert 'Metadata line' not in content
    assert 'pseudo_nhs_1' in content
    assert 'nhs_number' in content  # Column header should be present
    
    # Verify generate_output_key was called with 'sft'
    mock_generate_key.assert_called_once_with('sft')


def test_write_pseudonymised_data_raises_value_error_when_dataframe_empty():
    df = pd.DataFrame()

    with pytest.raises(ValueError) as exc_info:
        write_pseudonymised_data(df, 'test-output-bucket', 'test-kms-key', [], preserve_metadata=True, feed_type='gp')

    assert 'No records to write' in str(exc_info.value)


@patch('handler.datetime')
def test_generate_output_key_gp_feed(mock_datetime):
    from datetime import datetime
    mock_datetime.now.return_value = datetime(2025, 11, 6, 14, 30, 45, 123456)

    result = generate_output_key('gp')

    assert result == 'gp_feed/2025/11/06/raw/patient_20251106_143045_123456.csv'


@patch('handler.datetime')
def test_generate_output_key_sft_feed(mock_datetime):
    from datetime import datetime
    mock_datetime.now.return_value = datetime(2025, 11, 6, 14, 30, 45, 123456)

    result = generate_output_key('sft')

    assert result == 'sft_feed/2025/11/06/raw/patient_20251106_143045_123456.csv'


@pytest.mark.parametrize(
    "message,status_code,kwargs,expected_status,expected_message",
    [
        (
            "Pseudonymisation pipeline executed successfully",
            200,
            {
                'files_processed': 2,
                'total_records_input': 100,
                'total_records_valid': 95,
                'total_records_invalid': 5,
                'total_records_pseudonymised': 95
            },
            200,
            "Pseudonymisation pipeline executed successfully"
        ),
        (
            "Pseudonymisation pipeline execution failed: Lambda invocation error",
            500,
            {},
            500,
            "Pseudonymisation pipeline execution failed: Lambda invocation error"
        ),
    ],
)
def test_create_response(message, status_code, kwargs, expected_status, expected_message):
    import json

    result = create_response(message, status_code, **kwargs)

    assert result['statusCode'] == expected_status
    body = json.loads(result['body'])
    assert body['message'] == expected_message

    for key, value in kwargs.items():
        assert body[key] == value


@patch('handler.delete_s3_file')
@patch('handler.write_pseudonymised_data')
@patch('handler.pseudonymise')
@patch('handler.normalize_nhs_numbers')
@patch('handler.validate_dataframe')
@patch('handler.read_csv_from_s3')
def test_process_file_processes_successfully(
    mock_read_csv,
    mock_validate,
    mock_normalize,
    mock_pseudonymise,
    mock_write,
    mock_delete,
    gp_feed_config
):
    input_df = pd.DataFrame({
        'NHS Number': ['9434765919', '8314495581'],
        'Given Name': ['John', 'Jane'],
        'Family Name': ['Doe', 'Smith'],
        'Date of Birth': ['15-Jan-85', '20-Feb-90'],
        'Gender': ['Male', 'Female'],
        'Postcode': ['SW1A 1AA', 'M1 1AA']
    })

    validated_df = input_df.copy()
    normalized_df = input_df.copy()
    pseudonymised_df = input_df.copy()
    metadata_lines = ['Metadata line 1', 'Metadata line 2']

    mock_read_csv.return_value = (input_df, metadata_lines)
    mock_validate.return_value = (validated_df, [])
    mock_normalize.return_value = normalized_df
    mock_pseudonymise.return_value = pseudonymised_df

    result = process_file(
        'test-input-bucket',
        'test-key.csv',
        'test-output-bucket',
        'test-lambda-function',
        'test-kms-key',
        gp_feed_config
    )

    assert result['records_input'] == 2
    assert result['records_valid'] == 2
    assert result['records_invalid'] == 0
    assert result['records_pseudonymised'] == 2

    mock_write.assert_called_once()
    mock_delete.assert_called_once_with('test-input-bucket', 'test-key.csv')


@patch('handler.delete_s3_file')
@patch('handler.read_csv_from_s3')
def test_process_file_returns_zeros_when_empty_dataframe(mock_read_csv, mock_delete, gp_feed_config):
    mock_read_csv.return_value = (pd.DataFrame(), [])

    result = process_file(
        'test-input-bucket',
        'test-key.csv',
        'test-output-bucket',
        'test-lambda-function',
        'test-kms-key',
        gp_feed_config
    )

    assert result['records_input'] == 0
    assert result['records_valid'] == 0
    assert result['records_invalid'] == 0
    assert result['records_pseudonymised'] == 0

    mock_delete.assert_called_once_with('test-input-bucket', 'test-key.csv')


@patch('handler.delete_s3_file')
@patch('handler.validate_dataframe')
@patch('handler.read_csv_from_s3')
def test_process_file_returns_zeros_when_no_valid_records(mock_read_csv, mock_validate, mock_delete, gp_feed_config):
    input_df = pd.DataFrame({
        'NHS Number': ['1234567890', '0987654321'],
        'Given Name': ['John', 'Jane'],
        'Family Name': ['Doe', 'Smith'],
        'Date of Birth': ['15-Jan-85', '20-Feb-90'],
        'Gender': ['Male', 'Female'],
        'Postcode': ['SW1A 1AA', 'M1 1AA']
    })

    mock_read_csv.return_value = (input_df, ['Metadata 1', 'Metadata 2'])
    mock_validate.return_value = (pd.DataFrame(), [
        {'row_index': 0, 'error': 'Invalid NHS Number'},
        {'row_index': 1, 'error': 'Invalid NHS Number'}
    ])

    result = process_file(
        'test-input-bucket',
        'test-key.csv',
        'test-output-bucket',
        'test-lambda-function',
        'test-kms-key',
        gp_feed_config
    )

    assert result['records_input'] == 2
    assert result['records_valid'] == 0
    assert result['records_invalid'] == 2
    assert result['records_pseudonymised'] == 0

    mock_delete.assert_called_once_with('test-input-bucket', 'test-key.csv')


@patch('handler.pseudonymise')
@patch('handler.normalize_nhs_numbers')
@patch('handler.validate_dataframe')
@patch('handler.read_csv_from_s3')
def test_process_file_raises_value_error_when_record_count_mismatch_after_pseudonymisation(
    mock_read_csv,
    mock_validate,
    mock_normalize,
    mock_pseudonymise,
    gp_feed_config
):
    input_df = pd.DataFrame({
        'NHS Number': ['9434765919', '8314495581'],
        'Given Name': ['John', 'Jane'],
        'Family Name': ['Doe', 'Smith'],
        'Date of Birth': ['15-Jan-85', '20-Feb-90'],
        'Gender': ['Male', 'Female'],
        'Postcode': ['SW1A 1AA', 'M1 1AA']
    })

    validated_df = input_df.copy()
    normalized_df = input_df.copy()
    pseudonymised_df = pd.DataFrame({
        'NHS Number': ['pseudo_nhs_1'],
        'Given Name': ['pseudo_given_1'],
        'Family Name': ['pseudo_family_1'],
        'Date of Birth': ['pseudo_dob_1'],
        'Gender': ['pseudo_gender_1'],
        'Postcode': ['pseudo_postcode_1']
    })

    mock_read_csv.return_value = (input_df, ['Metadata 1', 'Metadata 2'])
    mock_validate.return_value = (validated_df, [])
    mock_normalize.return_value = normalized_df
    mock_pseudonymise.return_value = pseudonymised_df

    with pytest.raises(ValueError) as exc_info:
        process_file(
            'test-input-bucket',
            'test-key.csv',
            'test-output-bucket',
            'test-lambda-function',
            'test-kms-key',
            gp_feed_config
        )

    assert 'Record count mismatch after pseudonymisation' in str(exc_info.value)


@patch('handler.process_file')
@patch('handler.list_s3_files')
def test_process_all_files_processes_multiple_files(mock_list_files, mock_process_file, gp_feed_config):
    mock_list_files.return_value = ['file1.csv', 'file2.csv', 'file3.csv']

    mock_process_file.side_effect = [
        {
            'records_input': 50,
            'records_valid': 48,
            'records_invalid': 2,
            'records_pseudonymised': 48
        },
        {
            'records_input': 30,
            'records_valid': 30,
            'records_invalid': 0,
            'records_pseudonymised': 30
        },
        {
            'records_input': 20,
            'records_valid': 18,
            'records_invalid': 2,
            'records_pseudonymised': 18
        }
    ]

    result = process_all_files(
        'test-input-bucket',
        'test-prefix/',
        'test-output-bucket',
        'test-lambda-function',
        'test-kms-key',
        gp_feed_config
    )

    assert result['files_processed'] == 3
    assert result['total_records_input'] == 100
    assert result['total_records_valid'] == 96
    assert result['total_records_invalid'] == 4
    assert result['total_records_pseudonymised'] == 96

    assert mock_process_file.call_count == 3


@patch('handler.list_s3_files')
def test_process_all_files_returns_zeros_when_no_files(mock_list_files, gp_feed_config):
    mock_list_files.return_value = []

    result = process_all_files(
        'test-input-bucket',
        'test-prefix/',
        'test-output-bucket',
        'test-lambda-function',
        'test-kms-key',
        gp_feed_config
    )

    assert result['files_processed'] == 0
    assert result['total_records_input'] == 0
    assert result['total_records_valid'] == 0
    assert result['total_records_invalid'] == 0
    assert result['total_records_pseudonymised'] == 0


@patch('handler.get_feed_config')
@patch('handler.process_all_files')
@patch('handler.validate_required_params')
def test_lambda_handler_returns_success_response(mock_validate_params, mock_process_all, mock_get_feed_config, gp_feed_config):
    mock_validate_params.side_effect = [
        {
            'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda-function',
            'KMS_KEY_ID': 'test-kms-key'
        },
        {
            'input_s3_bucket': 'test-input-bucket',
            'input_prefix': 'test-prefix/',
            'output_s3_bucket': 'test-output-bucket',
            'feed_type': 'gp'
        }
    ]

    mock_get_feed_config.return_value = gp_feed_config

    mock_process_all.return_value = {
        'files_processed': 2,
        'total_records_input': 100,
        'total_records_valid': 95,
        'total_records_invalid': 5,
        'total_records_pseudonymised': 95
    }

    event = {
        'input_s3_bucket': 'test-input-bucket',
        'input_prefix': 'test-prefix/',
        'output_s3_bucket': 'test-output-bucket',
        'feed_type': 'gp'
    }

    result = lambda_handler(event, None)

    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['message'] == 'Pseudonymisation pipeline executed successfully'
    assert body['files_processed'] == 2
    assert body['total_records_input'] == 100
    assert body['total_records_valid'] == 95
    assert body['total_records_invalid'] == 5
    assert body['total_records_pseudonymised'] == 95


@patch('handler.get_feed_config')
@patch('handler.process_all_files')
@patch('handler.validate_required_params')
def test_lambda_handler_returns_success_response_when_no_files(mock_validate_params, mock_process_all, mock_get_feed_config, gp_feed_config):
    mock_validate_params.side_effect = [
        {
            'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda-function',
            'KMS_KEY_ID': 'test-kms-key'
        },
        {
            'input_s3_bucket': 'test-input-bucket',
            'input_prefix': 'test-prefix/',
            'output_s3_bucket': 'test-output-bucket',
            'feed_type': 'gp'
        }
    ]

    mock_get_feed_config.return_value = gp_feed_config

    mock_process_all.return_value = {
        'files_processed': 0,
        'total_records_input': 0,
        'total_records_valid': 0,
        'total_records_invalid': 0,
        'total_records_pseudonymised': 0
    }

    event = {
        'input_s3_bucket': 'test-input-bucket',
        'input_prefix': 'test-prefix/',
        'output_s3_bucket': 'test-output-bucket',
        'feed_type': 'gp'
    }

    result = lambda_handler(event, None)

    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['message'] == 'No CSV files found to process'
    assert body['files_processed'] == 0
    assert body['total_records_input'] == 0


@patch('handler.validate_required_params')
def test_lambda_handler_returns_error_response_on_exception(mock_validate_params):
    mock_validate_params.side_effect = ValueError('Missing required environment variables: PSEUDONYMISATION_LAMBDA_FUNCTION_NAME')

    event = {
        'input_s3_bucket': 'test-input-bucket',
        'input_prefix': 'test-prefix/',
        'output_s3_bucket': 'test-output-bucket',
        'feed_type': 'gp'
    }

    result = lambda_handler(event, None)

    assert result['statusCode'] == 500
    body = json.loads(result['body'])
    assert 'Pseudonymisation pipeline execution failed' in body['message']
    assert 'Missing required environment variables' in body['message']


@patch('handler.get_feed_config')
@patch('handler.process_all_files')
@patch('handler.validate_required_params')
def test_lambda_handler_sft_feed_success(mock_validate_params, mock_process_all, mock_get_feed_config, sft_feed_config):
    mock_validate_params.side_effect = [
        {
            'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda-function',
            'KMS_KEY_ID': 'test-kms-key'
        },
        {
            'input_s3_bucket': 'test-input-bucket',
            'input_prefix': 'sft-prefix/',
            'output_s3_bucket': 'test-output-bucket',
            'feed_type': 'sft'
        }
    ]

    mock_get_feed_config.return_value = sft_feed_config

    mock_process_all.return_value = {
        'files_processed': 3,
        'total_records_input': 150,
        'total_records_valid': 148,
        'total_records_invalid': 2,
        'total_records_pseudonymised': 148
    }

    event = {
        'input_s3_bucket': 'test-input-bucket',
        'input_prefix': 'sft-prefix/',
        'output_s3_bucket': 'test-output-bucket',
        'feed_type': 'sft'
    }

    result = lambda_handler(event, None)

    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['message'] == 'Pseudonymisation pipeline executed successfully'
    assert body['files_processed'] == 3
    assert body['total_records_input'] == 150
    assert body['total_records_valid'] == 148
    assert body['total_records_invalid'] == 2
    assert body['total_records_pseudonymised'] == 148

    # Verify get_feed_config was called with 'sft'
    mock_get_feed_config.assert_called_once_with('sft')

