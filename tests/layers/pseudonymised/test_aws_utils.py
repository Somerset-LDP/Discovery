import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from layers.pseudonymised.aws_utils import (
    list_s3_files,
    read_s3_file,
    write_to_s3,
    delete_s3_file,
    invoke_pseudonymisation_lambda_batch
)


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_list_s3_files_returns_csv_files(mock_s3_client):
    mock_paginator = MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            'Contents': [
                {'Key': 'prefix/file1.csv'},
                {'Key': 'prefix/file2.csv'},
                {'Key': 'prefix/'},
                {'Key': 'prefix/file3.txt'}
            ]
        }
    ]

    result = list_s3_files('test-bucket', 'prefix/')

    assert result == ['prefix/file1.csv', 'prefix/file2.csv']
    mock_s3_client.get_paginator.assert_called_once_with('list_objects_v2')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_list_s3_files_raises_client_error_on_failure(mock_s3_client):
    mock_s3_client.get_paginator.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchBucket', 'Message': 'Bucket not found'}},
        'ListObjectsV2'
    )

    with pytest.raises(ClientError):
        list_s3_files('test-bucket', 'prefix/')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_read_s3_file_returns_file_content(mock_s3_client):
    expected_content = b'test content'
    mock_s3_client.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=expected_content))
    }

    result = read_s3_file('test-bucket', 'test-key')

    assert result == expected_content
    mock_s3_client.get_object.assert_called_once_with(Bucket='test-bucket', Key='test-key')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_read_s3_file_raises_client_error_on_failure(mock_s3_client):
    mock_s3_client.get_object.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
        'GetObject'
    )

    with pytest.raises(ClientError):
        read_s3_file('test-bucket', 'test-key')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_write_to_s3_uploads_file_with_encryption(mock_s3_client):
    write_to_s3('test-bucket', 'test-key', 'test content', 'test-kms-key')

    mock_s3_client.put_object.assert_called_once_with(
        Bucket='test-bucket',
        Key='test-key',
        Body=b'test content',
        ServerSideEncryption='aws:kms',
        SSEKMSKeyId='test-kms-key'
    )


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_write_to_s3_raises_client_error_on_failure(mock_s3_client):
    mock_s3_client.put_object.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
        'PutObject'
    )

    with pytest.raises(ClientError):
        write_to_s3('test-bucket', 'test-key', 'test content', 'test-kms-key')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_delete_s3_file_deletes_successfully(mock_s3_client):
    delete_s3_file('test-bucket', 'test-key')

    mock_s3_client.delete_object.assert_called_once_with(Bucket='test-bucket', Key='test-key')


@patch('layers.pseudonymised.aws_utils.s3_client')
def test_delete_s3_file_raises_client_error_on_failure(mock_s3_client):
    mock_s3_client.delete_object.side_effect = ClientError(
        {'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
        'DeleteObject'
    )

    with pytest.raises(ClientError):
        delete_s3_file('test-bucket', 'test-key')


@patch('layers.pseudonymised.aws_utils.lambda_client')
def test_invoke_pseudonymisation_lambda_batch_returns_pseudonymised_values(mock_lambda_client):
    mock_response = {
        'Payload': MagicMock(read=MagicMock(return_value=json.dumps({
            'field_value': ['pseudo1', 'pseudo2', 'pseudo3']
        }).encode()))
    }
    mock_lambda_client.invoke.return_value = mock_response

    result = invoke_pseudonymisation_lambda_batch(
        'field_name',
        ['value1', 'value2', 'value3'],
        'test-lambda-function'
    )

    assert result == ['pseudo1', 'pseudo2', 'pseudo3']
    mock_lambda_client.invoke.assert_called_once()


@patch('layers.pseudonymised.aws_utils.lambda_client')
def test_invoke_pseudonymisation_lambda_batch_raises_value_error_on_lambda_error(mock_lambda_client):
    mock_response = {
        'Payload': MagicMock(read=MagicMock(return_value=json.dumps({
            'error': 'Pseudonymisation failed'
        }).encode()))
    }
    mock_lambda_client.invoke.return_value = mock_response

    with pytest.raises(ValueError) as exc_info:
        invoke_pseudonymisation_lambda_batch(
            'field_name',
            ['value1', 'value2'],
            'test-lambda-function'
        )

    assert 'Pseudonymisation Lambda returned error' in str(exc_info.value)
