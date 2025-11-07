"""
Revamped tests for cohort_membership module
Tests assume CSV files with single column, no header containing NHS numbers

Test Coverage:
read_cohort_members: Tests 1, 2, 3, 5
is_cohort_member: Tests 1, 2, 3, 4, 5
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
    return pd.Series(['1234567890', '2345678901', '3456789012'], name='nhs_numbers')


@pytest.fixture
def cohort_with_whitespace():
    """Return a pandas Series with NHS numbers that have whitespace"""
    return pd.Series(['  1234567890  ', '2345678901', '  3456789012', '4567890123  '], name='nhs_numbers')


# ==========================================
# read_cohort_members TESTS (Tests 1, 2, 3, 5)
# ==========================================

# Test 1: Valid single-column CSV file without header
def test_valid_single_column_no_header(test_data_dir):
    """
    Test 1: Valid single-column CSV file without header
    Test reading a properly formatted CSV file with multiple NHS numbers in a single column,
    ensuring all valid NHS numbers are returned as a pandas Series.
    """
    valid_file = test_data_dir / "valid_single_column.csv"
    result = read_cohort_members(str(valid_file))
    
    # Verify result type and content
    assert isinstance(result, pd.Series)
    assert len(result) == 5
    
    # Check all expected NHS numbers are present
    expected_nhs_numbers = ['1234567890', '2345678901', '3456789012', '4567890123', '5678901234']
    for nhs in expected_nhs_numbers:
        assert nhs in result.values
    
    # Verify all values are strings (preserving data type)
    assert all(isinstance(nhs, str) for nhs in result.values)
    
    # Verify no duplicates in this specific test file
    assert len(result) == len(set(result.values))


# Test 2: Empty or whitespace-only file handling
def test_completely_empty_file_raises_empty_data_error(test_data_dir):
    """
    Test 2a: Empty file handling
    Test that completely empty files raise EmptyDataError exception.
    """
    empty_file = test_data_dir / "completely_empty.csv"
    
    with pytest.raises(pd.errors.EmptyDataError, match="Cohort file appears to be empty"):
        read_cohort_members(str(empty_file))


def test_whitespace_only_file_raises_value_error(test_data_dir):
    """
    Test 2b: Whitespace-only file handling
    Test that files containing only whitespace raise ValueError for no valid NHS numbers.
    """
    whitespace_file = test_data_dir / "whitespace_only.csv"
    
    with pytest.raises(ValueError, match="Cohort file appears to be empty"):
        read_cohort_members(str(whitespace_file))


def test_all_null_values_raises_value_error(test_data_dir):
    """
    Test 2c: All null values handling
    Test that files containing only null/invalid values raise ValueError for no valid NHS numbers.
    """
    null_file = test_data_dir / "all_null_single_column.csv"
    
    with pytest.raises(ValueError, match="No valid NHS numbers found"):
        read_cohort_members(str(null_file))


# Test 3: Mixed valid and invalid data handling
def test_mixed_valid_and_invalid_data_filtering(test_data_dir):
    """
    Test 3: Mixed valid and invalid data handling
    Test a file containing a mix of valid NHS numbers, empty cells, whitespace-only cells,
    null values, and invalid entries to ensure only valid NHS numbers are returned.
    """
    mixed_file = test_data_dir / "mixed_valid_invalid.csv"
    result = read_cohort_members(str(mixed_file))
    
    # Should return only the 5 valid NHS numbers
    expected_valid = ['1234567890', '2345678901', '3456789012', '4567890123', '5678901234']
    assert len(result) == 5
    
    for nhs in expected_valid:
        assert nhs in result.values
    
    # Verify no null or invalid values are included
    assert not any(pd.isna(val) for val in result.values)
    result_lower = [val.strip().lower() for val in result.values]
    invalid_values = ['null', 'none', 'nan', '']
    assert not any(val in invalid_values for val in result_lower)
    
    # Verify all returned values are non-empty strings
    assert all(isinstance(val, str) and val.strip() != '' for val in result.values)


# Test 5: Data preservation and cleaning
def test_leading_zeros_preservation_and_whitespace_cleaning(test_data_dir):
    """
    Test 5a: Data preservation and cleaning - Leading zeros and whitespace
    Test that NHS numbers with leading zeros are preserved as strings and
    whitespace is properly stripped from NHS numbers.
    """
    leading_zeros_file = test_data_dir / "leading_zeros_whitespace.csv"
    result = read_cohort_members(str(leading_zeros_file))
    
    # Check that leading zeros are preserved
    assert '0123456789' in result.values
    assert '0234567890' in result.values
    assert '0345678901' in result.values
    assert '0456789012' in result.values
    
    # Check that whitespace is stripped from NHS numbers
    assert '1234567890' in result.values  # from "  1234567890  "
    assert '2345678901' in result.values  # from "  2345678901"
    assert '3456789012' in result.values  # from "3456789012  "
    assert '4567890123' in result.values  # from "  4567890123  "
    
    # Verify no values have leading/trailing whitespace
    assert all(val == val.strip() for val in result.values)
    
    # Verify all values are strings
    assert all(isinstance(val, str) for val in result.values)


def test_duplicate_nhs_numbers_preserved(test_data_dir):
    """
    Test 5b: Data preservation - Duplicates
    Test that duplicate NHS numbers are retained (not deduplicated) in the returned Series.
    """
    duplicates_file = test_data_dir / "duplicates_single_column.csv"
    result = read_cohort_members(str(duplicates_file))
    
    # Should preserve all entries including duplicates
    assert len(result) == 6  # Total entries including duplicates
    
    # Check specific duplicate counts
    duplicate_1234_count = (result == '1234567890').sum()
    duplicate_2345_count = (result == '2345678901').sum()
    single_3456_count = (result == '3456789012').sum()
    
    assert duplicate_1234_count == 3  # appears 3 times in file
    assert duplicate_2345_count == 2  # appears 2 times in file
    assert single_3456_count == 1     # appears once in file
    
    # Verify total count matches sum of individual counts
    assert duplicate_1234_count + duplicate_2345_count + single_3456_count == len(result)


# ==========================================
# is_cohort_member TESTS
# ==========================================

# Test 1: Basic membership validation
def test_nhs_number_exists_in_cohort_returns_true(valid_cohort_series):
    """
    Test 1a: Basic membership validation - NHS number exists
    Test that the function correctly returns True when an NHS number exists in the cohort Series.
    """
    assert is_cohort_member('1234567890', valid_cohort_series) is True
    assert is_cohort_member('2345678901', valid_cohort_series) is True
    assert is_cohort_member('3456789012', valid_cohort_series) is True


def test_nhs_number_not_in_cohort_returns_false(valid_cohort_series):
    """
    Test 1b: Basic membership validation - NHS number doesn't exist
    Test that the function correctly returns False when an NHS number doesn't exist in the cohort Series.
    """
    assert is_cohort_member('9999999999', valid_cohort_series) is False
    assert is_cohort_member('0000000000', valid_cohort_series) is False
    assert is_cohort_member('1111111111', valid_cohort_series) is False
    assert is_cohort_member('5555555555', valid_cohort_series) is False


# Test 2: Null and empty input handling
def test_null_nhs_number_returns_false_with_warning(valid_cohort_series, caplog):
    """
    Test 2a: Null and empty input handling - None input
    Test that null NHS numbers return False and generate appropriate warning logs.
    """
    with caplog.at_level(logging.WARNING):
        result = is_cohort_member(None, valid_cohort_series)  # type: ignore
        assert result is False
        assert "NHS number is None or empty" in caplog.text


def test_empty_string_nhs_number_returns_false_with_warning(valid_cohort_series, caplog):
    """
    Test 2b: Null and empty input handling - Empty string
    Test that empty string NHS numbers return False and generate appropriate warning logs.
    """
    with caplog.at_level(logging.WARNING):
        result = is_cohort_member('', valid_cohort_series)
        assert result is False
        assert "NHS number is None or empty" in caplog.text


def test_whitespace_only_nhs_number_returns_false_with_warning(valid_cohort_series, caplog):
    """
    Test 2c: Null and empty input handling - Whitespace-only strings
    Test that whitespace-only NHS numbers return False and generate appropriate warning logs.
    """
    # Test various whitespace patterns
    whitespace_inputs = ['   ', '\t\n  ', '  \t  ', '\n\r\t']
    
    for whitespace_input in whitespace_inputs:
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            result = is_cohort_member(whitespace_input, valid_cohort_series)
            assert result is False
            assert "NHS number is None or empty" in caplog.text


# Test 3: Whitespace normalization
def test_whitespace_normalization_input_nhs_number(cohort_with_whitespace):
    """
    Test 3a: Whitespace normalization - Input NHS number with whitespace
    Test that the function correctly matches NHS numbers even when there are whitespace
    differences between the input NHS number and the cohort members.
    """
    # Test input with extra whitespace matches cohort members
    assert is_cohort_member('  1234567890  ', cohort_with_whitespace) is True
    assert is_cohort_member('1234567890', cohort_with_whitespace) is True  # clean input
    assert is_cohort_member('   3456789012', cohort_with_whitespace) is True
    assert is_cohort_member('4567890123   ', cohort_with_whitespace) is True


def test_whitespace_normalization_cohort_members(cohort_with_whitespace):
    """
    Test 3b: Whitespace normalization - Cohort members with whitespace
    Test that clean input NHS numbers match cohort members that have whitespace.
    """
    # Clean input should match cohort members with whitespace
    assert is_cohort_member('1234567890', cohort_with_whitespace) is True
    assert is_cohort_member('2345678901', cohort_with_whitespace) is True
    assert is_cohort_member('3456789012', cohort_with_whitespace) is True
    assert is_cohort_member('4567890123', cohort_with_whitespace) is True
    
    # Should not match numbers not in cohort
    assert is_cohort_member('9999999999', cohort_with_whitespace) is False


# Test 4: Input validation and type checking
def test_invalid_cohort_members_list_raises_type_error():
    """
    Test 4a: Input validation - List instead of Series
    Test that the function raises a TypeError when the cohort_members parameter
    is not a pandas Series (e.g., when passed a list).
    """
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', ['1234567890', '2345678901'])  # type: ignore


def test_invalid_cohort_members_none_raises_type_error():
    """
    Test 4b: Input validation - None
    Test that the function raises a TypeError when cohort_members is None.
    """
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', None)  # type: ignore


def test_invalid_cohort_members_dataframe_raises_type_error():
    """
    Test 4c: Input validation - DataFrame instead of Series
    Test that the function raises a TypeError when cohort_members is a DataFrame.
    """
    df = pd.DataFrame({'nhs': ['1234567890', '2345678901']})
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', df)  # type: ignore


def test_invalid_cohort_members_string_raises_type_error():
    """
    Test 4d: Input validation - String instead of Series
    Test that the function raises a TypeError when cohort_members is a string.
    """
    with pytest.raises(TypeError, match="cohort_members must be a pandas Series"):
        is_cohort_member('1234567890', '1234567890,2345678901')  # type: ignore


# Test 5: Edge cases and error handling
def test_empty_cohort_series_returns_false():
    """
    Test 5a: Edge cases - Empty cohort Series
    Test behavior with empty cohort Series returns False.
    """
    empty_series = pd.Series([], dtype=str, name='nhs_numbers')
    result = is_cohort_member('1234567890', empty_series)
    assert result is False


def test_cohort_series_with_null_values_handles_gracefully():
    """
    Test 5b: Edge cases - Cohort Series with null values
    Test that the function handles cohort Series containing null values gracefully
    while still matching valid entries.
    """
    series_with_nulls = pd.Series(['1234567890', None, '2345678901', '', '3456789012'], name='nhs_numbers')
    
    # Should still match valid entries
    assert is_cohort_member('1234567890', series_with_nulls) is True
    assert is_cohort_member('2345678901', series_with_nulls) is True
    assert is_cohort_member('3456789012', series_with_nulls) is True
    
    # Should not match non-existent numbers
    assert is_cohort_member('9999999999', series_with_nulls) is False


def test_runtime_error_handling_for_unexpected_errors(valid_cohort_series):
    """
    Test 5c: Edge cases - Runtime error handling
    Test that the function handles any unexpected errors gracefully while providing
    meaningful error messages wrapped in RuntimeError.
    """
    # Mock a scenario that could cause an unexpected error during processing
    with patch.object(valid_cohort_series, 'astype', side_effect=Exception("Simulated unexpected error")):
        with pytest.raises(RuntimeError, match="Error checking cohort membership for NHS number 1234567890"):
            is_cohort_member('1234567890', valid_cohort_series)


def test_debug_logging_provides_detailed_information(valid_cohort_series, caplog):
    """
    Test 5d: Edge cases - Debug logging
    Test that debug logging provides detailed information about the cohort membership check process.
    """
    with caplog.at_level(logging.DEBUG):
        result = is_cohort_member('1234567890', valid_cohort_series)
        assert result is True
        
        # Check that debug logs contain expected information
        log_text = caplog.text
        assert "About to check if NHS number 1234567890 is in the cohort" in log_text
        assert "member count:3" in log_text
        assert "cohort member - 1234567890" in log_text
        assert "cohort member - 2345678901" in log_text
        assert "cohort member - 3456789012" in log_text
        assert "NHS number 1234567890 is member of cohort: True" in log_text


# ==========================================
# INTEGRATION TESTS
# ==========================================

def test_end_to_end_workflow_single_column(test_data_dir):
    """
    Integration test: Complete workflow with single-column file
    Test complete workflow: read single-column cohort then check membership
    """
    cohort_file = test_data_dir / "valid_single_column.csv"
    
    # Read cohort members
    cohort_members = read_cohort_members(str(cohort_file))
    
    # Test membership checking
    assert is_cohort_member('1234567890', cohort_members) is True
    assert is_cohort_member('5678901234', cohort_members) is True
    assert is_cohort_member('9999999999', cohort_members) is False


def test_end_to_end_with_leading_zeros(test_data_dir):
    """
    Integration test: Workflow preserves leading zeros
    Test workflow preserves leading zeros through read and check operations
    """
    cohort_file = test_data_dir / "leading_zeros_whitespace.csv"
    
    # Read cohort members
    cohort_members = read_cohort_members(str(cohort_file))
    
    # Test membership checking with leading zeros preserved
    assert is_cohort_member('0123456789', cohort_members) is True
    assert is_cohort_member('0234567890', cohort_members) is True
    assert is_cohort_member('1234567890', cohort_members) is True  # whitespace stripped
    assert is_cohort_member('123456789', cohort_members) is False   # different number (missing leading zero)


def test_end_to_end_with_duplicates(test_data_dir):
    """
    Integration test: Workflow with duplicates
    Test workflow with file containing duplicates works correctly for membership checking
    """
    cohort_file = test_data_dir / "duplicates_single_column.csv"
    
    # Read cohort members (preserves duplicates)
    cohort_members = read_cohort_members(str(cohort_file))
    
    # Test membership checking still works correctly with duplicates
    assert is_cohort_member('1234567890', cohort_members) is True
    assert is_cohort_member('2345678901', cohort_members) is True
    assert is_cohort_member('3456789012', cohort_members) is True
    assert is_cohort_member('9999999999', cohort_members) is False


# ==========================================
# ADDITIONAL EDGE CASE TESTS
# ==========================================

def test_single_nhs_number_file(test_data_dir):
    """Test reading a file with only one NHS number"""
    single_file = test_data_dir / "single_nhs_number.csv"
    result = read_cohort_members(str(single_file))
    
    assert len(result) == 1
    assert result.iloc[0] == '1234567890'
    
    # Test membership with single-member cohort
    assert is_cohort_member('1234567890', result) is True
    assert is_cohort_member('9999999999', result) is False


def test_mixed_with_nulls_file_processing(test_data_dir):
    """Test file that has valid NHS numbers mixed with nulls"""
    mixed_nulls_file = test_data_dir / "mixed_with_nulls.csv"
    result = read_cohort_members(str(mixed_nulls_file))
    
    # Should filter out nulls and return only valid NHS numbers
    expected_valid = ['1234567890', '2345678901', '3456789012']
    assert len(result) == 3
    for nhs in expected_valid:
        assert nhs in result.values


# ==========================================
# FILE ACCESS TESTS (Additional coverage)
# ==========================================

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
        f.write("1234567890")  # Single column, no header
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


# ==========================================
# REMOTE FILE ACCESS TESTS (with mocking)
# ==========================================

@patch('fsspec.open')
def test_http_url_success(mock_fsspec_open):
    """Test successful reading from HTTP URL"""
    import io
    # Mock single-column, no header content
    mock_content = "1234567890\n2345678901\n3456789012\n"
    mock_file = io.StringIO(mock_content)
    mock_fsspec_open.return_value.__enter__.return_value = mock_file
    
    result = read_cohort_members('https://example.com/cohort.csv')
    
    assert len(result) == 3
    assert '1234567890' in result.values
    assert '2345678901' in result.values
    assert '3456789012' in result.values


@patch('fsspec.open')
def test_s3_url_success(mock_fsspec_open):
    """Test successful reading from S3 URL"""
    import io
    # Mock single-column, no header content
    mock_content = "1234567890\n2345678901\n3456789012\n"
    mock_file = io.StringIO(mock_content)
    mock_fsspec_open.return_value.__enter__.return_value = mock_file
    
    result = read_cohort_members('s3://bucket/cohort.csv')
    
    assert len(result) == 3
    assert '1234567890' in result.values
    assert '2345678901' in result.values
    assert '3456789012' in result.values


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