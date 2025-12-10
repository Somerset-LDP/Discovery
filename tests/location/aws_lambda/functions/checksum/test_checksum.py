from unittest.mock import patch, MagicMock

import pytest

import location.aws_lambda.functions.checksum.checksum as checksum_module
from location.aws_lambda.functions.checksum.checksum import handler, is_duplicate
from location.aws_lambda.layers.common.common_utils import DataIngestionException
from location.aws_lambda.layers.common.db_utils import IngestRecord

VALID_S3_EVENT = {
    "Records": [{
        "eventTime": "2025-01-15T10:30:00.000Z",
        "s3": {
            "bucket": {"name": "ldp-zone-a-landing"},
            "object": {"key": "landing/reference/onspd/202402/ONSPD_FEB_2024_UK.csv"}
        }
    }]
}


@pytest.fixture(autouse=True)
def set_bronze_bucket_env():
    checksum_module.BRONZE_BUCKET = "test-bronze-bucket"


@pytest.fixture
def mock_s3_utils():
    with patch('location.aws_lambda.functions.checksum.checksum.get_s3_object_stream') as mock_stream, \
            patch('location.aws_lambda.functions.checksum.checksum.copy_s3_object') as mock_copy, \
            patch('location.aws_lambda.functions.checksum.checksum.delete_s3_object') as mock_delete:
        mock_stream.return_value = MagicMock()
        yield {"stream": mock_stream, "copy": mock_copy, "delete": mock_delete}


@pytest.fixture
def mock_db_utils():
    with patch('location.aws_lambda.functions.checksum.checksum.get_ingest_record') as mock_get, \
            patch('location.aws_lambda.functions.checksum.checksum.upsert_ingest_record') as mock_upsert:
        mock_get.return_value = None
        yield {"get": mock_get, "upsert": mock_upsert}


@pytest.fixture
def mock_checksum():
    with patch('location.aws_lambda.functions.checksum.checksum.calculate_sha256_checksum') as mock:
        mock.return_value = "abc123def456"
        yield mock


def test_handler_raises_exception_when_bronze_bucket_not_set():
    checksum_module.BRONZE_BUCKET = None

    with pytest.raises(DataIngestionException) as exc_info:
        handler(VALID_S3_EVENT, None)

    assert "BRONZE_BUCKET" in exc_info.value.message


@patch('location.aws_lambda.functions.checksum.checksum.parse_s3_event')
def test_handler_raises_exception_for_invalid_s3_event(mock_parse):
    mock_parse.side_effect = ValueError("Invalid S3 event structure")

    with pytest.raises(DataIngestionException) as exc_info:
        handler({}, None)

    assert "Unexpected error" in exc_info.value.message


@pytest.mark.parametrize("existing_record,checksum,expected", [
    (None, "abc123", False),
    (IngestRecord("ref/onspd", "file.csv", "different", "bronze_done", None), "abc123", False),
    (IngestRecord("ref/onspd", "file.csv", "abc123", "pending", None), "abc123", False),
    (IngestRecord("ref/onspd", "file.csv", "abc123", "bronze_done", None), "abc123", True),
    (IngestRecord("ref/onspd", "file.csv", "abc123", "silver_done", None), "abc123", True),
])
def test_is_duplicate_returns_correct_result(existing_record, checksum, expected):
    result = is_duplicate(existing_record, checksum)

    assert result == expected


def test_handler_skips_non_reference_path(mock_s3_utils, mock_db_utils, mock_checksum):
    event = {
        "Records": [{
            "eventTime": "2025-01-15T10:30:00.000Z",
            "s3": {
                "bucket": {"name": "ldp-zone-a-landing"},
                "object": {"key": "other/path/file.csv"}
            }
        }]
    }

    result = handler(event, None)

    assert result["status"] == "skipped"
    assert result["reason"] == "non-reference path"
    mock_s3_utils["stream"].assert_not_called()


def test_handler_processes_new_file_successfully(mock_s3_utils, mock_db_utils, mock_checksum):
    result = handler(VALID_S3_EVENT, None)

    assert result["status"] == "success"
    assert result["dataset_key"] == "reference/onspd"
    assert result["file_name"] == "ONSPD_FEB_2024_UK.csv"
    mock_s3_utils["copy"].assert_called_once()
    mock_s3_utils["delete"].assert_called_once()
    mock_db_utils["upsert"].assert_called_once()


def test_handler_skips_duplicate_file_and_deletes_from_landing(mock_s3_utils, mock_db_utils, mock_checksum):
    mock_checksum.return_value = "existing_checksum"
    mock_db_utils["get"].return_value = IngestRecord(
        dataset_key="reference/onspd",
        file_name="ONSPD_FEB_2024_UK.csv",
        checksum="existing_checksum",
        status="bronze_done",
        ingested_at=None
    )

    result = handler(VALID_S3_EVENT, None)

    assert result["status"] == "skipped"
    assert result["reason"] == "duplicate"
    mock_s3_utils["copy"].assert_not_called()
    mock_s3_utils["delete"].assert_called_once()
    mock_db_utils["upsert"].assert_not_called()
