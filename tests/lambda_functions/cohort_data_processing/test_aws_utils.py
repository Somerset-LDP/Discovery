import pytest
import json
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from lambda_functions.cohort_data_processing import aws_utils


def test_list_s3_files():
    mock_page = {
        'Contents': [
            {'Key': 'my-prefix/file1.csv'},
            {'Key': 'my-prefix/file2.csv'},
            {'Key': 'my-prefix/'},
        ]
    }
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [mock_page]
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.get_paginator.return_value = mock_paginator
        result = aws_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert result == ['my-prefix/file1.csv', 'my-prefix/file2.csv']


def test_list_s3_files_empty():
    mock_page = {'Contents': []}
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [mock_page]
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.get_paginator.return_value = mock_paginator
        result = aws_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert result == []


def test_list_s3_files_error():
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.get_paginator.side_effect = Exception("S3 error")
        with pytest.raises(Exception) as excinfo:
            aws_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert "S3 error" in str(excinfo.value)


def test_get_s3_object_content():
    mock_body = MagicMock()
    mock_body.read.return_value = b'data123'
    mock_response = {'Body': mock_body}
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.get_object.return_value = mock_response
        result = aws_utils.get_s3_object_content('my-bucket', 'my-key')
        assert result == b'data123'


def test_get_s3_object_content_error():
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.get_object.side_effect = Exception('S3 get error')
        with pytest.raises(Exception) as excinfo:
            aws_utils.get_s3_object_content('my-bucket', 'my-key')
        assert 'S3 get error' in str(excinfo.value)


def test_write_to_s3():
    nhs_set = {'123', '456'}
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_put = mock_client.put_object
        aws_utils.write_to_s3('my-bucket', 'my-key', nhs_set, 'my-kms-key')
        assert mock_put.call_count == 1
        args, kwargs = mock_put.call_args
        assert kwargs['Bucket'] == 'my-bucket'
        assert kwargs['Key'] == 'my-key'
        assert kwargs['ServerSideEncryption'] == 'aws:kms'
        assert kwargs['SSEKMSKeyId'] == 'my-kms-key'
        body = kwargs['Body'].decode('utf-8')
        for nhs in nhs_set:
            assert nhs in body


def test_write_to_s3_error():
    nhs_set = {'123'}
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.put_object.side_effect = Exception('S3 put error')
        with pytest.raises(Exception) as excinfo:
            aws_utils.write_to_s3('my-bucket', 'my-key', nhs_set, 'my-kms-key')
        assert 'S3 put error' in str(excinfo.value)


def test_delete_s3_objects():
    keys = ['file1.csv', 'file2.csv']
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_delete = mock_client.delete_object
        aws_utils.delete_s3_objects('my-bucket', keys)
        assert mock_delete.call_count == len(keys)
        called_keys = [call.kwargs['Key'] for call in mock_delete.call_args_list]
        for key in keys:
            assert key in called_keys


def test_delete_s3_objects_error():
    keys = ['file1.csv']
    with patch.object(aws_utils, 's3_client') as mock_client:
        mock_client.delete_object.side_effect = Exception('S3 delete error')
        with pytest.raises(Exception) as excinfo:
            aws_utils.delete_s3_objects('my-bucket', keys)
        assert 'S3 delete error' in str(excinfo.value)


def test_invoke_lambda_success():
    payload = {"action": "encrypt", "field_name": "nhs_number", "field_value": ["123", "456"]}
    expected_response = {"field_name": "nhs_number", "field_value": ["pseudo_1", "pseudo_2"]}

    mock_response_payload = MagicMock()
    mock_response_payload.read.return_value = json.dumps(expected_response).encode('utf-8')

    with patch.object(aws_utils, 'lambda_client') as mock_client:
        mock_client.invoke.return_value = {'Payload': mock_response_payload}

        result = aws_utils.invoke_lambda('test-function', payload)

        mock_client.invoke.assert_called_once_with(
            FunctionName='test-function',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )

        assert result == expected_response


def test_invoke_lambda_client_error():
    payload = {"action": "encrypt", "field_name": "test"}

    error_response = {
        'Error': {
            'Code': 'ResourceNotFoundException',
            'Message': 'Function not found'
        }
    }

    with patch.object(aws_utils, 'lambda_client') as mock_client:
        mock_client.invoke.side_effect = ClientError(error_response, 'Invoke')

        with pytest.raises(ClientError) as excinfo:
            aws_utils.invoke_lambda('non-existent-function', payload)

        assert 'ResourceNotFoundException' in str(excinfo.value)


def test_invoke_lambda_invalid_json_response():
    payload = {"action": "encrypt"}

    mock_response_payload = MagicMock()
    mock_response_payload.read.return_value = b'invalid-json{{'

    with patch.object(aws_utils, 'lambda_client') as mock_client:
        mock_client.invoke.return_value = {'Payload': mock_response_payload}

        with pytest.raises(ValueError) as excinfo:
            aws_utils.invoke_lambda('test-function', payload)

        assert "Invalid JSON response from Lambda" in str(excinfo.value)


def test_invoke_lambda_error_response():
    payload = {"action": "encrypt", "field_value": []}
    error_response = {"error": "Invalid input: field_value cannot be empty"}

    mock_response_payload = MagicMock()
    mock_response_payload.read.return_value = json.dumps(error_response).encode('utf-8')

    with patch.object(aws_utils, 'lambda_client') as mock_client:
        mock_client.invoke.return_value = {'Payload': mock_response_payload}

        result = aws_utils.invoke_lambda('test-function', payload)

        assert 'error' in result
        assert result['error'] == "Invalid input: field_value cannot be empty"
