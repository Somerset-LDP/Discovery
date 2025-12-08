from unittest.mock import patch

import pytest

from location.aws_lambda.functions.data_ingestion.data_ingestion import handler
from location.aws_lambda.layers.common.common_utils import DataIngestionException, DataIngestionSource
from aws_lambda_context import LambdaContext

VALID_TIMESTAMP = "2025-12-01T10:00:00.123"
VALID_BUCKET = "test-bucket"
ONSPD_SOURCE = DataIngestionSource.ONSPD.value
IMD_SOURCE = DataIngestionSource.IMD_2019.value


@pytest.fixture
def mock_context():
    return LambdaContext()


@pytest.fixture
def valid_onspd_event():
    return {
        "data_source": ONSPD_SOURCE,
        "s3_bucket": VALID_BUCKET,
        "ingestion-timestamp": VALID_TIMESTAMP
    }


@pytest.fixture
def valid_imd_event():
    return {
        "data_source": IMD_SOURCE,
        "s3_bucket": VALID_BUCKET,
        "ingestion-timestamp": VALID_TIMESTAMP
    }


@pytest.mark.parametrize("missing_field", ["data_source", "s3_bucket", "ingestion-timestamp"])
def test_handler_raises_exception_when_required_field_missing(missing_field, valid_onspd_event, mock_context):
    del valid_onspd_event[missing_field]

    with pytest.raises(DataIngestionException) as exc_info:
        handler(valid_onspd_event, mock_context)

    assert "Missing required event fields" in str(exc_info.value.message)
    assert missing_field in str(exc_info.value.message)


@pytest.mark.parametrize("invalid_source", ["invalid", "onspd_wrong", "", "imd"])
def test_handler_raises_exception_when_data_source_invalid(invalid_source, valid_onspd_event, mock_context):
    valid_onspd_event["data_source"] = invalid_source

    with pytest.raises(DataIngestionException) as exc_info:
        handler(valid_onspd_event, mock_context)

    assert "Invalid data_source" in str(exc_info.value.message)


@pytest.mark.parametrize("source,event_fixture,mock_func", [
    (ONSPD_SOURCE, "valid_onspd_event", "ingest_onspd_data"),
    (IMD_SOURCE, "valid_imd_event", "ingest_imd_data"),
])
@patch('location.aws_lambda.functions.data_ingestion.data_ingestion.ingest_imd_data')
@patch('location.aws_lambda.functions.data_ingestion.data_ingestion.ingest_onspd_data')
def test_handler_calls_correct_ingestion_function(mock_onspd, mock_imd, source, event_fixture, mock_func, request, mock_context):
    event = request.getfixturevalue(event_fixture)
    handler(event, mock_context)

    if source == ONSPD_SOURCE:
        mock_onspd.assert_called_once()
        mock_imd.assert_not_called()
        call_args = mock_onspd.call_args[0][0]
    else:
        mock_imd.assert_called_once()
        mock_onspd.assert_not_called()
        call_args = mock_imd.call_args[0][0]

    assert call_args.data_source == source
    assert call_args.target_bucket == VALID_BUCKET
    assert call_args.ingestion_timestamp == VALID_TIMESTAMP


@pytest.mark.parametrize("exception,message", [
    (DataIngestionException("Test error"), "Test error"),
    (RuntimeError("Unexpected error"), "Unexpected error"),
])
@patch('location.aws_lambda.functions.data_ingestion.data_ingestion.ingest_onspd_data')
def test_handler_propagates_exceptions(mock_ingest, exception, message, valid_onspd_event, mock_context):
    mock_ingest.side_effect = exception

    with pytest.raises(DataIngestionException) as exc_info:
        handler(valid_onspd_event, mock_context)

    assert message in str(exc_info.value.message)


def test_handler_raises_exception_for_empty_event(mock_context):
    with pytest.raises(DataIngestionException) as exc_info:
        handler({}, mock_context)

    assert "Missing required event fields" in str(exc_info.value.message)


