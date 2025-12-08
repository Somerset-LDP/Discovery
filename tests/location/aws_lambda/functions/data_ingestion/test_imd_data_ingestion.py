from io import BytesIO
from unittest.mock import patch

import pytest

from location.aws_lambda.functions.data_ingestion.imd_data_ingestion import ingest_imd_data
from location.aws_lambda.layers.common.common_utils import DataIngestionEvent, DataIngestionException

VALID_TIMESTAMP = "2025-12-01T10:00:00.123"
VALID_BUCKET = "test-bucket"
IMD_SOURCE = "imd_2019"
IMD_URL = "https://example.com/imd_file.xlsx"
IMD_TARGET_PREFIX = "File_1_IoD2025.xlsx"


@pytest.fixture
def valid_ingestion_event():
    return DataIngestionEvent(
        data_source=IMD_SOURCE,
        target_bucket=VALID_BUCKET,
        ingestion_timestamp=VALID_TIMESTAMP
    )


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("IMD_URL", IMD_URL)
    monkeypatch.setenv("IMD_TARGET_PREFIX", IMD_TARGET_PREFIX)


@pytest.mark.parametrize("missing_env_var", ["IMD_URL", "IMD_TARGET_PREFIX"])
def test_ingest_imd_data_raises_exception_when_env_var_missing(missing_env_var, valid_ingestion_event, monkeypatch):
    if missing_env_var == "IMD_URL":
        monkeypatch.setenv("IMD_TARGET_PREFIX", IMD_TARGET_PREFIX)
    else:
        monkeypatch.setenv("IMD_URL", IMD_URL)

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_imd_data(valid_ingestion_event)

    assert f"{missing_env_var} environment variable is not set" in str(exc_info.value.message)


@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.upload_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.download_file')
def test_ingest_imd_data_executes_full_flow_successfully(mock_download, mock_create_key, mock_upload,
                                                         valid_ingestion_event, mock_env_vars):
    xlsx_content = BytesIO(b"xlsx content")
    s3_key = "landing/reference/imd_2019/2025/12/01/File_1_IoD2025.xlsx"
    mock_download.return_value = xlsx_content
    mock_create_key.return_value = s3_key

    ingest_imd_data(valid_ingestion_event)

    mock_download.assert_called_once_with(IMD_URL, stream=False)
    mock_create_key.assert_called_once_with(IMD_SOURCE, VALID_TIMESTAMP, IMD_TARGET_PREFIX)
    mock_upload.assert_called_once_with(VALID_BUCKET, s3_key, xlsx_content)


@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.upload_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.download_file')
def test_ingest_imd_data_raises_exception_when_download_empty(mock_download, mock_create_key, mock_upload,
                                                              valid_ingestion_event, mock_env_vars):
    mock_download.return_value = None

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_imd_data(valid_ingestion_event)

    assert "Downloaded XLSX file is empty" in str(exc_info.value.message)


@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.upload_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.download_file')
def test_ingest_imd_data_propagates_download_exception(mock_download, mock_create_key, mock_upload,
                                                       valid_ingestion_event, mock_env_vars):
    mock_download.side_effect = DataIngestionException("Download failed")

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_imd_data(valid_ingestion_event)

    assert "Download failed" in str(exc_info.value.message)


@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.upload_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.download_file')
def test_ingest_imd_data_propagates_upload_exception(mock_download, mock_create_key, mock_upload, valid_ingestion_event,
                                                     mock_env_vars):
    mock_download.return_value = BytesIO(b"xlsx content")
    mock_create_key.return_value = "test-key"
    mock_upload.side_effect = DataIngestionException("Upload failed")

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_imd_data(valid_ingestion_event)

    assert "Upload failed" in str(exc_info.value.message)


@pytest.mark.parametrize("content_size", [1024, 1024 * 1024, 10 * 1024 * 1024])
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.upload_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.imd_data_ingestion.download_file')
def test_ingest_imd_data_handles_various_file_sizes(mock_download, mock_create_key, mock_upload, content_size,
                                                    valid_ingestion_event, mock_env_vars):
    xlsx_content = BytesIO(b"x" * content_size)
    mock_download.return_value = xlsx_content
    mock_create_key.return_value = "test-key"

    ingest_imd_data(valid_ingestion_event)

    mock_upload.assert_called_once()
    assert mock_upload.call_args[0][2] == xlsx_content
