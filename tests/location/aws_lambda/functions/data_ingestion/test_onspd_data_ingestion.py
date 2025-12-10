import zipfile
from io import BytesIO
from unittest.mock import patch

import pytest

from location.aws_lambda.functions.data_ingestion.onspd_data_ingestion import ingest_onspd_data
from location.aws_lambda.layers.common.common_utils import DataIngestionEvent, DataIngestionException

VALID_TIMESTAMP = "2025-12-01T10:00:00.123"
VALID_BUCKET = "test-bucket"
ONSPD_SOURCE = "onspd"
ONSPD_URL = "https://example.com/onspd.zip"
ONSPD_TARGET_PREFIX = "ONSPD_FEB_2024_UK/Data/ONSPD_FEB_2024_UK.csv"


@pytest.fixture
def valid_ingestion_event():
    return DataIngestionEvent(
        data_source=ONSPD_SOURCE,
        target_bucket=VALID_BUCKET,
        ingestion_timestamp=VALID_TIMESTAMP
    )


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("ONSPD_URL", ONSPD_URL)
    monkeypatch.setenv("ONSPD_TARGET_PREFIX", ONSPD_TARGET_PREFIX)


@pytest.fixture
def mock_zip_file():
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(ONSPD_TARGET_PREFIX, b"csv content data")
    zip_buffer.seek(0)
    return zip_buffer


@pytest.mark.parametrize("missing_env_var", ["ONSPD_URL", "ONSPD_TARGET_PREFIX"])
def test_ingest_onspd_data_raises_exception_when_env_var_missing(missing_env_var, valid_ingestion_event, monkeypatch):
    if missing_env_var == "ONSPD_URL":
        monkeypatch.setenv("ONSPD_TARGET_PREFIX", ONSPD_TARGET_PREFIX)
    else:
        monkeypatch.setenv("ONSPD_URL", ONSPD_URL)

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_onspd_data(valid_ingestion_event)

    assert f"{missing_env_var} environment variable is not set" in str(exc_info.value.message)


@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.upload_from_zip_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.download_file_to_temp')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.create_s3_key')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.os.path.exists')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.os.unlink')
def test_ingest_onspd_data_executes_full_flow_successfully(mock_unlink, mock_exists, mock_create_key,
                                                           mock_download, mock_upload,
                                                           valid_ingestion_event, mock_env_vars):
    s3_key = "landing/reference/onspd/2025/12/01/ONSPD_FEB_2024_UK.csv"
    temp_path = "/tmp/test.zip"

    mock_create_key.return_value = s3_key
    mock_download.return_value = temp_path
    mock_exists.return_value = True

    ingest_onspd_data(valid_ingestion_event)

    mock_create_key.assert_called_once_with(ONSPD_SOURCE, VALID_TIMESTAMP, "ONSPD_FEB_2024_UK.csv")
    mock_download.assert_called_once_with(ONSPD_URL, suffix='.zip')
    mock_upload.assert_called_once_with(temp_path, ONSPD_TARGET_PREFIX, VALID_BUCKET, s3_key)
    mock_unlink.assert_called_once_with(temp_path)


@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.upload_from_zip_to_s3')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.download_file_to_temp')
@patch('location.aws_lambda.functions.data_ingestion.onspd_data_ingestion.create_s3_key')
def test_ingest_onspd_data_propagates_exceptions(mock_create_key, mock_download, mock_upload,
                                                 valid_ingestion_event, mock_env_vars):
    mock_create_key.return_value = "test-key"
    mock_download.side_effect = DataIngestionException("Download failed")

    with pytest.raises(DataIngestionException) as exc_info:
        ingest_onspd_data(valid_ingestion_event)

    assert "Download failed" in str(exc_info.value.message)
