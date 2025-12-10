import os
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
import requests

from location.aws_lambda.layers.common.common import download_file, parse_to_datetime, download_file_to_temp
from location.aws_lambda.layers.common.common_utils import DataIngestionException


@pytest.fixture
def mock_response():
    response = MagicMock()
    response.status_code = 200
    response.content = b"file content"
    response.iter_content = MagicMock(return_value=[b"file content"])
    return response


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_downloads_successfully(mock_get, mock_response):
    mock_get.return_value = mock_response

    result = download_file("https://example.com/file.xlsx")

    assert result.read() == b"file content"
    mock_get.assert_called_once()


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_downloads_with_stream_false(mock_get, mock_response):
    mock_get.return_value = mock_response

    result = download_file("https://example.com/file.xlsx", stream=False)

    assert result.read() == b"file content"
    mock_get.assert_called_once()


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_raises_exception_for_empty_url(mock_get):
    with pytest.raises(DataIngestionException) as exc_info:
        download_file("")

    assert "URL is empty or None" in str(exc_info.value.message)
    mock_get.assert_not_called()


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_raises_exception_for_none_url(mock_get):
    with pytest.raises(DataIngestionException) as exc_info:
        download_file(None)

    assert "URL is empty or None" in str(exc_info.value.message)
    mock_get.assert_not_called()


@pytest.mark.parametrize("status_code", [400, 404, 500, 503])
@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_raises_exception_for_non_200_status(mock_get, mock_response, status_code):
    mock_response.status_code = status_code
    mock_get.return_value = mock_response

    with pytest.raises(DataIngestionException) as exc_info:
        download_file("https://example.com/file.zip")

    assert f"HTTP status: {status_code}" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_raises_exception_for_empty_content(mock_get, mock_response):
    mock_response.content = b""
    mock_response.iter_content = MagicMock(return_value=[])
    mock_response.headers = {"Content-Type": "application/zip"}
    mock_get.return_value = mock_response

    with pytest.raises(DataIngestionException) as exc_info:
        download_file("https://example.com/file.zip")

    assert "is empty" in str(exc_info.value.message)


@pytest.mark.parametrize("exception_class,error_msg", [
    (requests.exceptions.Timeout, "Timeout downloading"),
    (requests.exceptions.ConnectionError, "Connection error downloading"),
    (requests.exceptions.RequestException, "Request error downloading"),
])
@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_handles_requests_exceptions(mock_get, exception_class, error_msg):
    mock_get.side_effect = exception_class("Test error")

    with pytest.raises(DataIngestionException) as exc_info:
        download_file("https://example.com/file.zip")

    assert error_msg in str(exc_info.value.message)


@pytest.mark.parametrize("timestamp,expected_year,expected_month,expected_day", [
    ("2025-12-01T10:00:00.123", 2025, 12, 1),
    ("2025-01-15T00:00:00.000", 2025, 1, 15),
    ("2025-06-30T23:59:59.999", 2025, 6, 30),
    ("2025-12-01T10:00:00.123456", 2025, 12, 1),
    ("2025-12-01T10:00:00", 2025, 12, 1),
    ("2025-12-01 10:00:00.123456", 2025, 12, 1),
    ("2025-12-01 10:00:00", 2025, 12, 1),
    ("2025-12-01", 2025, 12, 1),
    ("2025-01-01", 2025, 1, 1),
])
def test_parse_to_datetime_parses_various_formats(timestamp, expected_year, expected_month, expected_day):
    result = parse_to_datetime(timestamp)

    assert isinstance(result, datetime)
    assert result.year == expected_year
    assert result.month == expected_month
    assert result.day == expected_day


@pytest.mark.parametrize("timestamp", [
    "2025-12-01T10:00:00.123456Z",
    "2025-12-01T10:00:00Z",
    "2025-12-01Z",
])
def test_parse_to_datetime_handles_trailing_z(timestamp):
    result = parse_to_datetime(timestamp)

    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 12
    assert result.day == 1


@pytest.mark.parametrize("invalid_timestamp", [
    "invalid",
    "2025-13-01",
    "2025-12-32",
    "not a timestamp",
    "2025/12/01",
    "01-12-2025",
])
def test_parse_to_datetime_raises_exception_for_invalid_format(invalid_timestamp):
    with pytest.raises(DataIngestionException) as exc_info:
        parse_to_datetime(invalid_timestamp)

    assert "Invalid ingestion_timestamp format" in str(exc_info.value.message)


@pytest.mark.parametrize("empty_timestamp", ["", "   ", None])
def test_parse_to_datetime_raises_exception_for_empty_input(empty_timestamp):
    with pytest.raises(DataIngestionException) as exc_info:
        parse_to_datetime(empty_timestamp)

    assert "ingestion_timestamp is empty or None" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_to_temp_downloads_successfully(mock_get, mock_response):
    mock_get.return_value = mock_response
    temp_path = download_file_to_temp("https://example.com/file.zip", suffix='.zip')

    try:
        assert temp_path is not None
        assert os.path.exists(temp_path)
        assert temp_path.endswith('.zip')
        mock_get.assert_called_once()
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_to_temp_raises_exception_for_empty_url(mock_get):
    with pytest.raises(DataIngestionException) as exc_info:
        download_file_to_temp("")

    assert "URL is empty or None" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_to_temp_raises_exception_on_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    with pytest.raises(DataIngestionException) as exc_info:
        download_file_to_temp("https://example.com/file.zip")

    assert "HTTP status: 404" in str(exc_info.value.message)


@patch('location.aws_lambda.layers.common.common.requests.get')
def test_download_file_to_temp_cleans_up_on_error(mock_get):
    mock_get.side_effect = requests.exceptions.Timeout()

    with pytest.raises(DataIngestionException):
        download_file_to_temp("https://example.com/file.zip")
