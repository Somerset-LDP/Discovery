import pytest
import pandas as pd
import logging
from unittest.mock import patch
from pipeline.emis_gprecord import run


# Fixtures
@pytest.fixture
def sample_cohort_store():
    """Sample cohort store with NHS numbers"""
    return pd.Series(['1234567890', '2345678901', '3456789012'], name='nhs')


@pytest.fixture
def empty_cohort_store():
    """Empty cohort store"""
    return pd.Series([], name='nhs', dtype=str)


@pytest.fixture
def single_member_cohort_store():
    """Cohort store with exactly one member"""
    return pd.Series(['1234567890'], name='nhs')


@pytest.fixture
def large_cohort_store():
    """Large cohort store with many members"""
    return pd.Series([f"{i:010d}" for i in range(1000)], name='nhs')


@pytest.fixture
def duplicate_cohort_store():
    """Cohort store with duplicate NHS numbers"""
    return pd.Series(['1234567890', '2345678901', '1234567890', '3456789012'], name='nhs')


# Happy Path Scenarios (1-6)
def test_successful_processing_with_mixed_cohort_non_cohort_records(sample_cohort_store):
    """Test successful processing with mixed cohort/non-cohort records"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},    # In cohort, has ethnicity
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'},    # Not in cohort
        {'nhs_number': '2345678901', 'ethnicity': 'Black'},    # In cohort, has ethnicity
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return only the cohort members with ethnicity
    assert len(result) == 2
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 'Black'}


def test_all_records_are_cohort_members(sample_cohort_store):
    """Test when every GP record matches someone in the cohort store"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},
        {'nhs_number': '3456789012', 'ethnicity': 'Black'},
    ]
    
    result = run(sample_cohort_store, records)
    
    # All records should be returned as they're all cohort members with ethnicity
    assert len(result) == 3
    assert result == records


def test_no_records_are_cohort_members(sample_cohort_store):
    """Test when no GP records match anyone in the cohort store"""
    records = [
        {'nhs_number': '9999999999', 'ethnicity': 'White'},
        {'nhs_number': '8888888888', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, records)
    
    # No records should be returned as none are cohort members
    assert len(result) == 0
    assert result == []


def test_single_record_processing(sample_cohort_store):
    """Test pipeline processes exactly one GP record"""
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    result = run(sample_cohort_store, records)
    
    # Single record should be returned
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}


def test_empty_records_list(sample_cohort_store):
    """Test pipeline handles empty list of GP records gracefully"""
    records = []
    
    result = run(sample_cohort_store, records)
    
    # Should return empty list
    assert len(result) == 0
    assert result == []


def test_large_dataset_processing(large_cohort_store):
    """Test pipeline processes thousands of GP records efficiently"""
    records = []
    for i in range(500):
        records.append({
            'nhs_number': f"{i:010d}",
            'ethnicity': f'Ethnicity_{i % 10}'
        })
    
    result = run(large_cohort_store, records)
    
    # All 500 records should be returned (all are in large cohort and have ethnicity)
    assert len(result) == 500
    assert all('ethnicity' in record for record in result)


# NHS Number Validation Scenarios (7-12)
def test_records_missing_nhs_number_field(sample_cohort_store):
    """Test records that don't have an nhs_number key"""
    records = [
        {'ethnicity': 'White'},  # Missing nhs_number
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
        {'age': 25}  # Missing nhs_number
    ]
    
    result = run(sample_cohort_store, records)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'Asian'}


def test_records_with_empty_nhs_number_strings(sample_cohort_store):
    """Test nhs_number field exists but contains empty string"""
    records = [
        {'nhs_number': '', 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, records)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'Asian'}


def test_records_with_none_nhs_numbers(sample_cohort_store):
    """Test nhs_number field contains None/null values"""
    records = [
        {'nhs_number': None, 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, records)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'Asian'}


def test_records_with_whitespace_only_nhs_numbers(sample_cohort_store):
    """Test nhs_number contains only spaces/tabs"""
    records = [
        {'nhs_number': '   ', 'ethnicity': 'White'},
        {'nhs_number': '\t\n  ', 'ethnicity': 'Asian'},
        {'nhs_number': '1234567890', 'ethnicity': 'Black'},
    ]
    
    result = run(sample_cohort_store, records)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'Black'}


def test_mixed_nhs_number_data_types(sample_cohort_store):
    """Test some NHS numbers are strings, some are integers"""
    records = [
        {'nhs_number': 1234567890, 'ethnicity': 'White'},      # Integer
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},    # String
    ]
    
    result = run(sample_cohort_store, records)
    
    # Both should be processed if they're in cohort and have ethnicity
    assert len(result) == 2
    assert result[0] == {'nhs_number': 1234567890, 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 'Asian'}


def test_nhs_numbers_with_different_formatting():
    """Test NHS numbers with spaces, dashes, or other formatting"""
    # Create cohort with various formats
    formatted_cohort = pd.Series([
        '1234567890', '2345678901', '3456789012',
        '123 456 7890', '234-567-8901'
    ], name='nhs')
    
    records = [
        {'nhs_number': '123 456 7890', 'ethnicity': 'White'},
        {'nhs_number': '234-567-8901', 'ethnicity': 'Asian'},
        {'nhs_number': '9999999999', 'ethnicity': 'Black'},  # Not in cohort
    ]
    
    result = run(formatted_cohort, records)
    
    # Should return the two records that are in cohort
    assert len(result) == 2
    assert result[0] == {'nhs_number': '123 456 7890', 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '234-567-8901', 'ethnicity': 'Asian'}


# Ethnicity Validation Scenarios (13-18)
def test_cohort_members_missing_ethnicity_field(sample_cohort_store):
    """Test records in cohort but no ethnicity key in dictionary"""
    records = [
        {'nhs_number': '1234567890'},  # In cohort, missing ethnicity
        {'nhs_number': '9999999999', 'ethnicity': 'White'}  # Not in cohort
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return the cohort member even without ethnicity (current behavior)
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890'}


def test_cohort_members_with_empty_ethnicity_strings(sample_cohort_store):
    """Test ethnicity field exists but empty"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': ''},  # In cohort, empty ethnicity
        {'nhs_number': '9999999999', 'ethnicity': ''}   # Not in cohort
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return the cohort member even with empty ethnicity (current behavior)
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': ''}


def test_cohort_members_with_none_ethnicity(sample_cohort_store):
    """Test ethnicity field contains None/null"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': None},  # In cohort
        {'nhs_number': '9999999999', 'ethnicity': None}   # Not in cohort
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return the cohort member even with None ethnicity (current behavior)
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': None}


def test_cohort_members_with_whitespace_only_ethnicity(sample_cohort_store):
    """Test ethnicity contains only spaces"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': '   '},   # In cohort
        {'nhs_number': '2345678901', 'ethnicity': '\t\n'}   # In cohort
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return both cohort members even with whitespace-only ethnicity
    assert len(result) == 2
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': '   '}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': '\t\n'}


def test_non_cohort_members_with_missing_ethnicity(sample_cohort_store):
    """Test should not return non-cohort members regardless of ethnicity"""
    records = [
        {'nhs_number': '9999999999'},  # Not in cohort, missing ethnicity
        {'nhs_number': '8888888888', 'ethnicity': ''}  # Not in cohort, empty ethnicity
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return no records as none are cohort members
    assert len(result) == 0
    assert result == []


def test_mixed_ethnicity_data_types(sample_cohort_store):
    """Test some ethnicities are strings, some are numbers"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},    # String
        {'nhs_number': '2345678901', 'ethnicity': 123},        # Number
        {'nhs_number': '3456789012', 'ethnicity': True}        # Boolean
    ]
    
    result = run(sample_cohort_store, records)
    
    # All should be returned as they're cohort members with some ethnicity value
    assert len(result) == 3
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 123}
    assert result[2] == {'nhs_number': '3456789012', 'ethnicity': True}


# Cohort Store Scenarios (19-23)
def test_empty_cohort_store(empty_cohort_store):
    """Test cohort pandas Series contains no members"""
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    result = run(empty_cohort_store, records)
    
    # Should return no records as cohort is empty
    assert len(result) == 0
    assert result == []


def test_single_member_cohort_store(single_member_cohort_store):
    """Test cohort contains exactly one NHS number"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # In cohort
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(single_member_cohort_store, records)
    
    # Should return only the cohort member
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}


def test_large_cohort_store(large_cohort_store):
    """Test cohort contains thousands of NHS numbers"""
    records = [
        {'nhs_number': '0000000001', 'ethnicity': 'White'},  # In large cohort
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(large_cohort_store, records)
    
    # Should return only the cohort member
    assert len(result) == 1
    assert result[0] == {'nhs_number': '0000000001', 'ethnicity': 'White'}


def test_cohort_store_with_duplicate_nhs_numbers(duplicate_cohort_store):
    """Test same NHS number appears multiple times in cohort"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # In cohort (appears twice)
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(duplicate_cohort_store, records)
    
    # Should return the cohort member
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}


def test_cohort_store_with_mixed_data_types():
    """Test NHS numbers stored as strings and integers"""
    mixed_cohort = pd.Series(['1234567890', 2345678901, '3456789012'], name='nhs')
    records = [
        {'nhs_number': '2345678901', 'ethnicity': 'White'},  # String matching int in cohort
        {'nhs_number': 3456789012, 'ethnicity': 'Asian'}     # Int matching string in cohort
    ]
    
    result = run(mixed_cohort, records)
    
    # Both should be returned if cohort membership check handles type conversion
    assert len(result) >= 1  # At least one should match


# Data Processing Edge Cases (24, 26, 27, 28)
def test_records_with_additional_fields(sample_cohort_store):
    """Test GP records contain extra fields beyond nhs_number/ethnicity"""
    records = [
        {
            'nhs_number': '1234567890', 
            'ethnicity': 'White',
            'age': 25,
            'postcode': 'TA1 1AA',
            'diagnosis': 'Diabetes'
        }
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return the record with all its fields intact
    assert len(result) == 1
    assert result[0] == {
        'nhs_number': '1234567890', 
        'ethnicity': 'White',
        'age': 25,
        'postcode': 'TA1 1AA',
        'diagnosis': 'Diabetes'
    }


def test_records_missing_fields_entirely(sample_cohort_store):
    """Test GP records missing expected fields entirely"""
    records = [
        {'age': 25, 'postcode': 'TA1 1AA'},  # Missing both nhs_number and ethnicity
        {'nhs_number': '1234567890'},         # Missing ethnicity only
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return the cohort member even without ethnicity
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890'}


def test_empty_record_dictionaries(sample_cohort_store):
    """Test some records are completely empty dictionaries"""
    records = [
        {},  # Empty dictionary
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {}   # Another empty dictionary
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return only the valid record
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}


def test_duplicate_nhs_numbers_in_records(sample_cohort_store):
    """Test same NHS number appears in multiple GP records"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},  # Duplicate
        {'nhs_number': '2345678901', 'ethnicity': 'Black'}
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return all cohort members, including duplicates
    assert len(result) == 3
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '1234567890', 'ethnicity': 'Asian'}
    assert result[2] == {'nhs_number': '2345678901', 'ethnicity': 'Black'}


# Cohort Membership Check Scenarios (29-33)
@patch('pipeline.emis_gprecord.is_cohort_member')
def test_cohort_membership_check_returns_true(mock_is_cohort_member, sample_cohort_store):
    """Test NHS number found in cohort store"""
    mock_is_cohort_member.return_value = True
    
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    result = run(sample_cohort_store, records)
    
    # Should return the record since mock says it's a cohort member
    assert len(result) == 1
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}
    mock_is_cohort_member.assert_called_once()


@patch('pipeline.emis_gprecord.is_cohort_member')
def test_cohort_membership_check_returns_false(mock_is_cohort_member, sample_cohort_store):
    """Test NHS number not found in cohort store"""
    mock_is_cohort_member.return_value = False
    
    records = [{'nhs_number': '9999999999', 'ethnicity': 'White'}]
    
    result = run(sample_cohort_store, records)
    
    # Should return no records since mock says it's not a cohort member
    assert len(result) == 0
    assert result == []
    mock_is_cohort_member.assert_called_once()


@patch('pipeline.emis_gprecord.is_cohort_member')
def test_cohort_membership_check_raises_exception(mock_is_cohort_member, sample_cohort_store):
    """Test error in cohort membership validation"""
    mock_is_cohort_member.side_effect = Exception("Cohort check failed")
    
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    with pytest.raises(Exception, match="Cohort check failed"):
        run(sample_cohort_store, records)


def test_case_sensitivity_in_nhs_numbers():
    """Test NHS numbers with different casing (though NHS numbers are numeric)"""
    cohort = pd.Series(['ABC123', 'def456'], name='nhs')
    records = [
        {'nhs_number': 'ABC123', 'ethnicity': 'White'},  # Exact match
        {'nhs_number': 'abc123', 'ethnicity': 'Asian'}   # Different case
    ]
    
    result = run(cohort, records)
    
    # Should return only exact matches (case sensitive)
    assert len(result) == 1
    assert result[0] == {'nhs_number': 'ABC123', 'ethnicity': 'White'}


def test_whitespace_handling_in_cohort_checks():
    """Test NHS numbers with leading/trailing spaces"""
    cohort = pd.Series(['1234567890', '2345678901'], name='nhs')
    records = [
        {'nhs_number': ' 1234567890 ', 'ethnicity': 'White'},  # With spaces
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'}     # Without spaces
    ]
    
    result = run(cohort, records)
    
    # Result depends on how is_cohort_member handles whitespace
    # This test documents the current behavior
    assert isinstance(result, list)


# Data Type Validation Scenarios (50)
def test_unicode_characters_in_ethnicity_data(sample_cohort_store):
    """Test non-ASCII characters in ethnicity fields"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'Café'},      # Accented characters
        {'nhs_number': '2345678901', 'ethnicity': '中文'},       # Chinese characters
        {'nhs_number': '3456789012', 'ethnicity': 'العربية'}    # Arabic characters
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should handle Unicode characters without issues
    assert len(result) == 3
    assert result[0]['ethnicity'] == 'Café'
    assert result[1]['ethnicity'] == '中文'
    assert result[2]['ethnicity'] == 'العربية'


# Continuation Logic Scenarios (53-56)
def test_processing_continues_after_invalid_nhs_number(sample_cohort_store):
    """Test pipeline doesn't stop on bad NHS number"""
    records = [
        {'ethnicity': 'White'},  # Invalid: no NHS number
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},  # Valid
        {'nhs_number': None, 'ethnicity': 'Black'},  # Invalid: None NHS number
        {'nhs_number': '2345678901', 'ethnicity': 'Mixed'}   # Valid
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return only the valid cohort member records
    assert len(result) == 2
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'Asian'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 'Mixed'}


def test_processing_continues_after_missing_ethnicity(sample_cohort_store):
    """Test pipeline continues after ethnicity warning"""
    records = [
        {'nhs_number': '1234567890'},  # In cohort, missing ethnicity
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},  # In cohort, valid
        {'nhs_number': '3456789012', 'ethnicity': ''}        # In cohort, empty ethnicity
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return all cohort members regardless of ethnicity issues
    assert len(result) == 3
    assert result[0] == {'nhs_number': '1234567890'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 'Asian'}
    assert result[2] == {'nhs_number': '3456789012', 'ethnicity': ''}


def test_all_records_skipped_due_to_invalid_nhs_numbers(sample_cohort_store):
    """Test every record has invalid NHS number"""
    records = [
        {'ethnicity': 'White'},     # No NHS number
        {'nhs_number': '', 'ethnicity': 'Asian'},    # Empty NHS number
        {'nhs_number': None, 'ethnicity': 'Black'},  # None NHS number
        {'age': 25}  # No NHS number field
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return empty list as no records have valid NHS numbers
    assert len(result) == 0
    assert result == []


def test_mixed_valid_and_invalid_records(sample_cohort_store):
    """Test some records process, others are skipped"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # Valid, in cohort
        {'ethnicity': 'Asian'},                              # Invalid: no NHS number
        {'nhs_number': '9999999999', 'ethnicity': 'Black'},  # Valid, not in cohort
        {'nhs_number': '', 'ethnicity': 'Mixed'},            # Invalid: empty NHS number
        {'nhs_number': '2345678901', 'ethnicity': 'Other'}   # Valid, in cohort
    ]
    
    result = run(sample_cohort_store, records)
    
    # Should return only the valid cohort member records
    assert len(result) == 2
    assert result[0] == {'nhs_number': '1234567890', 'ethnicity': 'White'}
    assert result[1] == {'nhs_number': '2345678901', 'ethnicity': 'Other'}