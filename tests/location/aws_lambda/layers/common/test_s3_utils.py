import os
import tempfile
import zipfile
from io import BytesIO
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from location.aws_lambda.layers.common.common_utils import DataIngestionException, CHUNK_SIZE_BYTES
from location.aws_lambda.layers.common.s3_utils import (
    create_s3_key,
    upload_to_s3,
    upload_to_s3_multipart,
    upload_from_zip_to_s3,
    parse_s3_event
)

VALID_TIMESTAMP = "2025-12-01T10:00:00.123"
VALID_DATA_SOURCE = "onspd"
VALID_FILE_NAME = "test_file.csv"
VALID_BUCKET = "test-bucket"


@pytest.mark.parametrize("data_source,timestamp,file_name,expected_key", [
    ("onspd", "2025-12-01T10:00:00.123", "ONSPD_FEB_2024_UK.csv",
     "landing/reference/onspd/2025/12/01/ONSPD_FEB_2024_UK.csv"),
    ("imd_2019", "2025-01-15T00:00:00.000", "File_1_IoD2025.xlsx",
     "landing/reference/imd_2019/2025/01/15/File_1_IoD2025.xlsx"),
    ("test", "2025-06-30T23:59:59.999", "data.csv", "landing/reference/test/2025/06/30/data.csv"),
])
def test_create_s3_key_generates_correct_path(data_source, timestamp, file_name, expected_key):
    result = create_s3_key(data_source, timestamp, file_name)

    assert result == expected_key


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
def test_create_s3_key_raises_exception_for_invalid_data_source(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        create_s3_key(invalid_value, VALID_TIMESTAMP, VALID_FILE_NAME)

    assert "data_source" in str(exc_info.value.message)
    assert "is null or empty" in str(exc_info.value.message)


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
def test_create_s3_key_raises_exception_for_invalid_file_name(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        create_s3_key(VALID_DATA_SOURCE, VALID_TIMESTAMP, invalid_value)

    assert "file_name" in str(exc_info.value.message)
    assert "is null or empty" in str(exc_info.value.message)


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
def test_create_s3_key_raises_exception_for_invalid_timestamp(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        create_s3_key(VALID_DATA_SOURCE, invalid_value, VALID_FILE_NAME)

    assert "ingestion_timestamp" in str(exc_info.value.message)
    assert "is null or empty" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_uploads_bytes_successfully(mock_s3_client):
    content = b"test content"
    s3_key = "test/key.csv"

    upload_to_s3(VALID_BUCKET, s3_key, content)

    mock_s3_client.put_object.assert_called_once_with(
        Bucket=VALID_BUCKET,
        Key=s3_key,
        Body=content
    )


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_uploads_bytesio_successfully(mock_s3_client):
    content = BytesIO(b"test content")
    s3_key = "test/key.csv"

    upload_to_s3(VALID_BUCKET, s3_key, content)

    mock_s3_client.put_object.assert_called_once()
    call_args = mock_s3_client.put_object.call_args
    assert call_args[1]["Bucket"] == VALID_BUCKET
    assert call_args[1]["Key"] == s3_key
    assert call_args[1]["Body"] == b"test content"


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_raises_exception_for_invalid_bucket(mock_s3_client, invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(invalid_value, "test/key.csv", b"content")

    assert "S3 bucket name is null or empty" in str(exc_info.value.message)
    mock_s3_client.put_object.assert_not_called()


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_raises_exception_for_invalid_key(mock_s3_client, invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(VALID_BUCKET, invalid_value, b"content")

    assert "S3 key is null or empty" in str(exc_info.value.message)
    mock_s3_client.put_object.assert_not_called()


@pytest.mark.parametrize("invalid_value", [None, b""])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_raises_exception_for_invalid_content(mock_s3_client, invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(VALID_BUCKET, "test/key.csv", invalid_value)

    assert "Content is null or empty" in str(exc_info.value.message)
    mock_s3_client.put_object.assert_not_called()


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_raises_exception_for_empty_body_after_conversion(mock_s3_client):
    content = BytesIO(b"")

    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(VALID_BUCKET, "test/key.csv", content)

    assert "Content body is empty after conversion" in str(exc_info.value.message)


@pytest.mark.parametrize("error_code,error_message", [
    ("NoSuchBucket", "The specified bucket does not exist"),
    ("AccessDenied", "Access Denied"),
    ("InvalidBucketName", "The specified bucket is not valid"),
])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_handles_client_errors(mock_s3_client, error_code, error_message):
    error_response = {
        'Error': {
            'Code': error_code,
            'Message': error_message
        }
    }
    mock_s3_client.put_object.side_effect = ClientError(error_response, 'PutObject')

    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(VALID_BUCKET, "test/key.csv", b"content")

    assert "S3 upload failed" in str(exc_info.value.message)
    assert error_code in str(exc_info.value.message)
    assert error_message in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_handles_unexpected_exceptions(mock_s3_client):
    mock_s3_client.put_object.side_effect = RuntimeError("Unexpected error")

    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3(VALID_BUCKET, "test/key.csv", b"content")

    assert "Unexpected error during upload" in str(exc_info.value.message)


@pytest.mark.parametrize("content_size", [100, 1024, 1024 * 1024])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_handles_various_content_sizes(mock_s3_client, content_size):
    content = b"x" * content_size

    upload_to_s3(VALID_BUCKET, "test/key.csv", content)

    mock_s3_client.put_object.assert_called_once()
    assert mock_s3_client.put_object.call_args[1]["Body"] == content


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_uploads_successfully(mock_s3_client):
    content = BytesIO(b"x" * (CHUNK_SIZE_BYTES + 1000))
    s3_key = "test/large_file.csv"

    mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_s3_client.upload_part.return_value = {'ETag': 'test-etag'}

    upload_to_s3_multipart(VALID_BUCKET, s3_key, content)

    mock_s3_client.create_multipart_upload.assert_called_once_with(Bucket=VALID_BUCKET, Key=s3_key)
    assert mock_s3_client.upload_part.call_count == 2
    mock_s3_client.complete_multipart_upload.assert_called_once()


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_uploads_single_part(mock_s3_client):
    content = BytesIO(b"small content")
    s3_key = "test/file.csv"

    mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_s3_client.upload_part.return_value = {'ETag': 'test-etag'}

    upload_to_s3_multipart(VALID_BUCKET, s3_key, content)

    mock_s3_client.create_multipart_upload.assert_called_once()
    mock_s3_client.upload_part.assert_called_once()
    mock_s3_client.complete_multipart_upload.assert_called_once()


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_raises_exception_for_invalid_bucket(mock_s3_client, invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3_multipart(invalid_value, "test/key.csv", BytesIO(b"content"))

    assert "S3 bucket name is null or empty" in str(exc_info.value.message)


@pytest.mark.parametrize("invalid_value", ["", "   ", None])
@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_raises_exception_for_invalid_key(mock_s3_client, invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3_multipart(VALID_BUCKET, invalid_value, BytesIO(b"content"))

    assert "S3 key is null or empty" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_raises_exception_for_empty_stream(mock_s3_client):
    content = BytesIO(b"")

    mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}

    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3_multipart(VALID_BUCKET, "test/key.csv", content)

    assert "No data to upload" in str(exc_info.value.message)
    mock_s3_client.abort_multipart_upload.assert_called_once()


@patch('location.aws_lambda.layers.common.s3_utils.s3_client')
def test_upload_to_s3_multipart_aborts_on_upload_error(mock_s3_client):
    content = BytesIO(b"test content")

    mock_s3_client.create_multipart_upload.return_value = {'UploadId': 'test-upload-id'}
    mock_s3_client.upload_part.side_effect = ClientError(
        {'Error': {'Code': 'InternalError', 'Message': 'Internal error'}},
        'UploadPart'
    )

    with pytest.raises(DataIngestionException) as exc_info:
        upload_to_s3_multipart(VALID_BUCKET, "test/key.csv", content)

    mock_s3_client.abort_multipart_upload.assert_called_once()
    assert "S3 multipart upload failed" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.s3_utils.upload_to_s3_multipart')
def test_upload_from_zip_to_s3_uploads_successfully(mock_upload):
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.zip') as temp_zip:
        temp_zip_path = temp_zip.name
        with zipfile.ZipFile(temp_zip, 'w') as zip_file:
            zip_file.writestr("folder/test.csv", b"test csv content")

    try:
        upload_from_zip_to_s3(temp_zip_path, "folder/test.csv", VALID_BUCKET, "test/key.csv")

        mock_upload.assert_called_once()
        assert mock_upload.call_args[0][0] == VALID_BUCKET
        assert mock_upload.call_args[0][1] == "test/key.csv"
    finally:
        if os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)


@pytest.mark.parametrize("invalid_param,value,error_msg", [
    ("zip_file_path", None, "ZIP file path is invalid"),
    ("zip_file_path", "/nonexistent/file.zip", "ZIP file path is invalid"),
    ("target_file_path", None, "target_file_path is empty or None"),
    ("target_file_path", "", "target_file_path is empty or None"),
    ("s3_bucket", None, "S3 bucket name is null or empty"),
    ("s3_bucket", "", "S3 bucket name is null or empty"),
])
def test_upload_from_zip_to_s3_raises_exception_for_invalid_params(invalid_param, value, error_msg):
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.zip') as temp_zip:
        temp_zip_path = temp_zip.name
        with zipfile.ZipFile(temp_zip, 'w') as zip_file:
            zip_file.writestr("test.csv", b"test content")

    try:
        params = {
            "zip_file_path": temp_zip_path,
            "target_file_path": "test.csv",
            "s3_bucket": VALID_BUCKET,
            "s3_key": "test/key.csv"
        }
        params[invalid_param] = value

        with pytest.raises(DataIngestionException) as exc_info:
            upload_from_zip_to_s3(**params)

        assert error_msg in str(exc_info.value.message)
    finally:
        if os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)


def test_upload_from_zip_to_s3_raises_exception_for_missing_file_in_zip():
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.zip') as temp_zip:
        temp_zip_path = temp_zip.name
        with zipfile.ZipFile(temp_zip, 'w') as zip_file:
            zip_file.writestr("other.csv", b"other content")

    try:
        with pytest.raises(DataIngestionException) as exc_info:
            upload_from_zip_to_s3(temp_zip_path, "missing.csv", VALID_BUCKET, "test/key.csv")

        assert "not found in ZIP archive" in str(exc_info.value.message)
    finally:
        if os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)


def test_upload_from_zip_to_s3_raises_exception_for_empty_file():
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.zip') as temp_zip:
        temp_zip_path = temp_zip.name
        with zipfile.ZipFile(temp_zip, 'w') as zip_file:
            zip_file.writestr("empty.csv", b"")

    try:
        with pytest.raises(DataIngestionException) as exc_info:
            upload_from_zip_to_s3(temp_zip_path, "empty.csv", VALID_BUCKET, "test/key.csv")

        assert "exists but is empty" in str(exc_info.value.message)
    finally:
        if os.path.exists(temp_zip_path):
            os.unlink(temp_zip_path)


def test_parse_s3_event_extracts_bucket_key_and_timestamp():
    event = {
        "Records": [{
            "eventTime": "2025-01-15T10:30:00.000Z",
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "landing/reference/onspd/file.csv"}
            }
        }]
    }

    result = parse_s3_event(event)

    assert result.bucket == "test-bucket"
    assert result.key == "landing/reference/onspd/file.csv"
    assert result.ingestion_timestamp == "2025-01-15T10:30:00.000Z"


def test_parse_s3_event_decodes_url_encoded_key():
    event = {
        "Records": [{
            "eventTime": "2025-01-15T10:30:00.000Z",
            "s3": {
                "bucket": {"name": "test-bucket"},
                "object": {"key": "landing/reference/folder+with+spaces/file%2B1.csv"}
            }
        }]
    }

    result = parse_s3_event(event)

    assert result.key == "landing/reference/folder with spaces/file+1.csv"


@pytest.mark.parametrize("invalid_event", [
    {},
    {"Records": []},
    {"Records": [{"s3": {}}]},
    {"Records": [{"s3": {"bucket": {}}}]},
    {"Records": [{"s3": {"bucket": {"name": "bucket"}, "object": {}}}]},
])
def test_parse_s3_event_raises_exception_for_invalid_event(invalid_event):
    with pytest.raises(ValueError):
        parse_s3_event(invalid_event)
