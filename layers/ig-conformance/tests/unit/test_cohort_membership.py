"""
Tests for cohort_membership module
"""
import pytest
import pandas as pd
import logging
from pathlib import Path
from unittest.mock import patch
import tempfile
import os

from common.cohort_membership import read_cohort_members, is_cohort_member


# Test fixtures
@pytest.fixture
def test_data_dir():
    """Return path to test data directory"""
    return Path(__file__).parent.parent / "fixtures" / "cohort_data"


@pytest.fixture
def valid_cohort_series():
    """Return a valid pandas Series with NHS numbers for testing is_cohort_member"""
    return pd.Series(['1234567890', '2345678901', '3456789012'], name='nhs')


# File/URL access tests
def test_read_valid_local_file(test_data_dir):
    """Test reading a valid local CSV file"""
    valid_cohort_file = test_data_dir / "valid_cohort.csv"
    result = read_cohort_members(str(valid_cohort_file))
    
    assert isinstance(result, pd.Series)
    assert len(result) == 5
    assert '1234567890' in result.values
    assert '5678901234' in result.values


def test_read_with_file_protocol(test_data_dir):
    """Test reading with file:// protocol"""
    valid_cohort_file = test_data_dir / "valid_cohort.csv"
    file_url = f"file://{valid_cohort_file}"
    result = read_cohort_members(file_url)
    
    assert isinstance(result, pd.Series)
    assert len(result) == 5


def test_file_not_found():
    """Test FileNotFoundError for non-existent file"""
    with pytest.raises(FileNotFoundError, match="Cohort file not found"):
        read_cohort_members("/non/existent/path.csv")


def test_file_not_found_with_file_protocol():
    """Test FileNotFoundError with file:// protocol"""
    with pytest.raises(FileNotFoundError):
        read_cohort_members("file:///non/existent/path.csv")


def test_permission_denied():
    """Test PermissionError for unreadable file"""
    # Create a temporary file and remove read permissions
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
        f.write("nhs,name\n1234567890,Test")
        temp_file = f.name
    
    try:
        # Remove read permissions
        os.chmod(temp_file, 0o000)
        
        with pytest.raises(PermissionError, match="Access denied"):
            read_cohort_members(temp_file)
    finally:
        # Restore permissions and cleanup
        os.chmod(temp_file, 0o644)
        os.unlink(temp_file)


# CSV content and column validation tests
def test_empty_file(test_data_dir):
    """Test EmptyDataError for completely empty file"""
    empty_file = test_data_dir / "empty_file.csv"
    
    with pytest.raises(pd.errors.EmptyDataError, match="Cohort file appears to be empty"):
        read_cohort_members(str(empty_file))


def test_headers_only_file(test_data_dir):
    """Test ValueError for file with headers but no data"""
    headers_only = test_data_dir / "headers_only.csv"
    
    with pytest.raises(ValueError, match="No data found in cohort file"):
        read_cohort_members(str(headers_only))


def test_malformed_csv(test_data_dir):
    """Test ParserError for malformed CSV"""
    malformed = test_data_dir / "malformed.csv"
    
    with pytest.raises(pd.errors.ParserError, match="Error parsing cohort file"):
        read_cohort_members(str(malformed))


# NHS number validation and data quality tests
def test_mixed_null_values(test_data_dir):
    """Test handling of mixed null and valid NHS numbers"""
    mixed_null = test_data_dir / "mixed_null_values.csv"
    result = read_cohort_members(str(mixed_null))
    
    # Should return only non-null values
    assert len(result) == 3  # Only 3 valid NHS numbers
    assert '1234567890' in result.values
    assert '3456789012' in result.values
    assert '5678901234' in result.values


def test_all_null_nhs_values(test_data_dir):
    """Test ValueError when all NHS numbers are null"""
    all_null = test_data_dir / "all_null_nhs.csv"
    
    with pytest.raises(ValueError, match="No valid NHS numbers found"):
        read_cohort_members(str(all_null))


def test_single_row_file(test_data_dir):
    """Test handling of CSV with single NHS number"""
    single_row = test_data_dir / "single_row.csv"
    result = read_cohort_members(str(single_row))
    
    assert len(result) == 1
    assert result.iloc[0] == '1234567890'


def test_duplicate_nhs_numbers(test_data_dir):
    """Test that duplicate NHS numbers are preserved"""
    duplicates = test_data_dir / "duplicates.csv"
    result = read_cohort_members(str(duplicates))
    
    # Should return all values including duplicates
    assert len(result) == 5
    # Count occurrences of duplicated NHS number
    duplicate_count = (result == '1234567890').sum()
    assert duplicate_count == 2


def test_whitespace_handling(test_data_dir):
    """Test handling of whitespace in NHS numbers"""
    whitespace = test_data_dir / "whitespace_issues.csv"
    result = read_cohort_members(str(whitespace))
    
    # Should handle whitespace gracefully
    assert len(result) == 3  # Only valid non-null entries
    # Note: Current implementation may not strip whitespace - this tests actual behavior


def test_special_characters(test_data_dir):
    """Test handling of NHS numbers with special characters"""
    special_chars = test_data_dir / "special_characters.csv"
    result = read_cohort_members(str(special_chars))
    
    assert len(result) == 4
    assert 'NHS234567890' in result.values
    assert '3456-789-012' in result.values


# HTTP/S3 URL scenarios with mocking
@patch('fsspec.open')
def test_http_url_success(mock_fsspec_open):
    """Test successful reading from HTTP URL"""
    # Mock successful HTTP response
    import io
    mock_content = "nhs,name,dob\n1234567890,John Smith,1980-01-15\n2345678901,Jane Doe,1975-06-22\n"
    mock_file = io.StringIO(mock_content)
    mock_fsspec_open.return_value.__enter__.return_value = mock_file
    
    result = read_cohort_members('https://example.com/cohort.csv')
    
    assert len(result) == 2
    assert '1234567890' in result.values
    assert '2345678901' in result.values


@patch('fsspec.open')
def test_s3_url_success(mock_fsspec_open):
    """Test successful reading from S3 URL"""
    # Mock successful S3 response
    import io
    mock_content = "nhs,name,dob\n1234567890,John Smith,1980-01-15\n2345678901,Jane Doe,1975-06-22\n"
    mock_file = io.StringIO(mock_content)
    mock_fsspec_open.return_value.__enter__.return_value = mock_file
    
    result = read_cohort_members('s3://bucket/cohort.csv')
    
    assert len(result) == 2
    assert '1234567890' in result.values
    assert '2345678901' in result.values


@patch('fsspec.open')
def test_connection_error(mock_fsspec_open):
    """Test ConnectionError for network issues"""
    mock_fsspec_open.side_effect = ConnectionError("Network unreachable")
    
    with pytest.raises(ConnectionError, match="Unable to connect to location"):
        read_cohort_members('https://example.com/cohort.csv')


@patch('fsspec.open')
def test_timeout_error(mock_fsspec_open):
    """Test TimeoutError handling"""
    mock_fsspec_open.side_effect = TimeoutError("Request timeout")
    
    with pytest.raises(ConnectionError, match="Unable to connect to location"):
        read_cohort_members('https://example.com/cohort.csv')


# Cohort membership checking function tests
def test_nhs_number_in_cohort(valid_cohort_series):
    """Test NHS number that is in the cohort"""
    result = is_cohort_member('1234567890', valid_cohort_series)
    assert result is True


def test_nhs_number_not_in_cohort(valid_cohort_series):
    """Test NHS number that is not in the cohort"""
    result = is_cohort_member('9999999999', valid_cohort_series)
    assert result is False


def test_empty_nhs_number(valid_cohort_series, caplog):
    """Test that empty NHS numbers return False and log warning"""
    with caplog.at_level(logging.WARNING):
        result = is_cohort_member('', valid_cohort_series)
        assert result is False
        assert "NHS number is None or empty" in caplog.text
    
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        result = is_cohort_member(None, valid_cohort_series)  # type: ignore
        assert result is False
        assert "NHS number is None or empty" in caplog.text


def test_whitespace_only_nhs_number(valid_cohort_series, caplog):
    """Test that whitespace-only NHS numbers return False and log warning"""
    with caplog.at_level(logging.WARNING):
        result = is_cohort_member('   ', valid_cohort_series)
        assert result is False
        assert "NHS number is None or empty" in caplog.text


def test_invalid_cohort_members_type():
    """Test TypeError for invalid cohort_members parameter"""
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', ['1234567890', '2345678901'])  # type: ignore
    
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', None)  # type: ignore


def test_empty_cohort_series():
    """Test handling of empty cohort series"""
    empty_series = pd.Series([], dtype=str, name='nhs')
    result = is_cohort_member('1234567890', empty_series)
    assert result is False


def test_whitespace_matching():
    """Test that whitespace is handled correctly in matching"""
    cohort_with_spaces = pd.Series(['  1234567890  ', '2345678901'], name='nhs')
    
    # Should match even with whitespace differences
    result = is_cohort_member('1234567890', cohort_with_spaces)
    assert result is True
    
    result = is_cohort_member('  1234567890  ', cohort_with_spaces)
    assert result is True


# Integration tests combining read_cohort_members and is_cohort_member
def test_end_to_end_workflow(test_data_dir):
    """Test complete workflow: read cohort then check membership"""
    valid_cohort_file = test_data_dir / "valid_cohort.csv"
    # Read cohort members
    cohort_members = read_cohort_members(str(valid_cohort_file))
    
    # Test membership checking
    assert is_cohort_member('1234567890', cohort_members) is True
    assert is_cohort_member('9999999999', cohort_members) is False


def test_with_duplicates_file(test_data_dir):
    """Test workflow with file containing duplicates"""
    duplicates_file = test_data_dir / "duplicates.csv"
    cohort_members = read_cohort_members(str(duplicates_file))
    
    # Should still work correctly with duplicates
    assert is_cohort_member('1234567890', cohort_members) is True
    assert is_cohort_member('9999999999', cohort_members) is False