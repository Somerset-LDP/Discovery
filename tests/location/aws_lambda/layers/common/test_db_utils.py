from datetime import datetime
from unittest.mock import patch

import pytest

from location.aws_lambda.layers.common.common_utils import DataIngestionException, DataIngestionStatus
from location.aws_lambda.layers.common.db_utils import get_db_credentials, get_ingest_record, upsert_ingest_record
import location.aws_lambda.layers.common.db_utils as db_utils


@pytest.fixture(autouse=True)
def reset_cached_credentials():
    db_utils.cached_username = None
    db_utils.cached_password = None
    yield
    db_utils.cached_username = None
    db_utils.cached_password = None


@patch.dict('os.environ', {
    'LDP_DB_USERNAME_SECRET': 'ldp/db_username',
    'LDP_DB_PASSWORD_SECRET': 'ldp/db_password'
})
@patch('location.aws_lambda.layers.common.db_utils.get_secret_value')
def test_get_db_credentials_fetches_from_secrets_manager(mock_get_secret):
    mock_get_secret.side_effect = ['test_user', 'test_pass']

    username, password = get_db_credentials()

    assert username == 'test_user'
    assert password == 'test_pass'
    assert mock_get_secret.call_count == 2


@patch.dict('os.environ', {}, clear=True)
def test_get_db_credentials_raises_exception_when_secrets_missing():
    with pytest.raises(DataIngestionException) as exc_info:
        get_db_credentials()

    assert "LDP_DB_USERNAME_SECRET" in exc_info.value.message


@pytest.mark.parametrize("invalid_value", ["", None])
def test_get_ingest_record_raises_exception_for_invalid_dataset_key(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        get_ingest_record(invalid_value, "file.csv")

    assert "dataset_key and file_name are required" in exc_info.value.message


@pytest.mark.parametrize("invalid_value", ["", None])
def test_get_ingest_record_raises_exception_for_invalid_file_name(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        get_ingest_record("reference/onspd", invalid_value)

    assert "dataset_key and file_name are required" in exc_info.value.message


@pytest.mark.parametrize("missing_field", ["dataset_key", "file_name", "checksum"])
def test_upsert_ingest_record_raises_exception_for_missing_required_fields(missing_field):
    params = {
        "dataset_key": "reference/onspd",
        "file_name": "file.csv",
        "checksum": "abc123",
        "status": DataIngestionStatus.BRONZE_DONE,
        "ingested_at": datetime.now()
    }
    params[missing_field] = ""

    with pytest.raises(DataIngestionException) as exc_info:
        upsert_ingest_record(**params)

    assert "dataset_key, file_name, and checksum are required" in exc_info.value.message
