import pytest
from unittest.mock import patch, MagicMock
from lambda_functions.cohort_data_processing import s3_utils


def test_list_s3_files():
    mock_response = {
        'Contents': [
            {'Key': 'my-prefix/file1.csv'},
            {'Key': 'my-prefix/file2.csv'},
            {'Key': 'my-prefix/'},
        ]
    }
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.list_objects_v2.return_value = mock_response
        result = s3_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert result == ['my-prefix/file1.csv', 'my-prefix/file2.csv']


def test_list_s3_files_empty():
    mock_response = {'Contents': []}
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.list_objects_v2.return_value = mock_response
        result = s3_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert result == []


def test_list_s3_files_error():
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.list_objects_v2.side_effect = Exception('S3 error')
        with pytest.raises(Exception) as excinfo:
            s3_utils.list_s3_files('my-bucket', 'my-prefix/')
        assert 'S3 error' in str(excinfo.value)


def test_get_s3_object_content():
    mock_body = MagicMock()
    mock_body.read.return_value = b'data123'
    mock_response = {'Body': mock_body}
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.get_object.return_value = mock_response
        result = s3_utils.get_s3_object_content('my-bucket', 'my-key')
        assert result == b'data123'


def test_get_s3_object_content_error():
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.get_object.side_effect = Exception('S3 get error')
        with pytest.raises(Exception) as excinfo:
            s3_utils.get_s3_object_content('my-bucket', 'my-key')
        assert 'S3 get error' in str(excinfo.value)


def test_write_to_s3():
    nhs_set = {'123', '456'}
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_put = mock_client.put_object
        s3_utils.write_to_s3('my-bucket', 'my-key', nhs_set)
        assert mock_put.call_count == 1
        args, kwargs = mock_put.call_args
        assert kwargs['Bucket'] == 'my-bucket'
        assert kwargs['Key'] == 'my-key'
        body = kwargs['Body'].decode('utf-8')
        for nhs in nhs_set:
            assert nhs in body


def test_write_to_s3_error():
    nhs_set = {'123'}
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.put_object.side_effect = Exception('S3 put error')
        with pytest.raises(Exception) as excinfo:
            s3_utils.write_to_s3('my-bucket', 'my-key', nhs_set)
        assert 'S3 put error' in str(excinfo.value)


def test_delete_s3_objects():
    keys = ['file1.csv', 'file2.csv']
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_delete = mock_client.delete_object
        s3_utils.delete_s3_objects('my-bucket', keys)
        assert mock_delete.call_count == len(keys)
        called_keys = [call.kwargs['Key'] for call in mock_delete.call_args_list]
        for key in keys:
            assert key in called_keys


def test_delete_s3_objects_error():
    keys = ['file1.csv']
    with patch.object(s3_utils, 's3_client') as mock_client:
        mock_client.delete_object.side_effect = Exception('S3 delete error')
        with pytest.raises(Exception) as excinfo:
            s3_utils.delete_s3_objects('my-bucket', keys)
        assert 'S3 delete error' in str(excinfo.value)
