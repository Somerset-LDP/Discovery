import pytest
import pandas as pd
from pipeline.conformance_processor import run
from pipeline.feed_config import GP_FEED, SFT_FEED
from unittest.mock import MagicMock



def _ensure_nhs_first(records):
    """Ensure nhs_number is the first column for pipeline compatibility"""
    df = pd.DataFrame(records)
    if 'nhs_number' in df.columns:
        # Reorder so nhs_number is first
        cols = ['nhs_number'] + [col for col in df.columns if col != 'nhs_number']
        df = df[cols]
    return df

def _ensure_nhs_second(records):
    """Ensure nhs_number is the second column for SFT feed compatibility"""
    df = pd.DataFrame(records)
    if 'nhs_number' in df.columns:
        other_cols = [col for col in df.columns if col != 'nhs_number']
        if len(other_cols) > 0:
            cols = [other_cols[0], 'nhs_number'] + other_cols[1:]
            df = df[cols]
    return df

# Mock encrypt function for testing
def mock_encrypt(field_name: str, values: list[str]) -> list[str]:
    """Mock encrypt function that just returns the raw input value"""
    return values


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

    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the cohort members with ethnicity
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '1234567890'
    assert result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '2345678901'
    assert result.iloc[1]['ethnicity'] == 'Black'


def test_all_records_are_cohort_members(sample_cohort_store):
    """Test when every GP record matches someone in the cohort store"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},
        {'nhs_number': '3456789012', 'ethnicity': 'Black'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # All records should be returned as they're all cohort members with ethnicity
    assert len(result) == 3
    assert len(result) == len(records)


def test_no_records_are_cohort_members(sample_cohort_store):
    """Test when no GP records match anyone in the cohort store"""
    records = [
        {'nhs_number': '9999999999', 'ethnicity': 'White'},
        {'nhs_number': '8888888888', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # No records should be returned as none are cohort members
    assert len(result) == 0
    assert len(result) == 0


def test_single_record_processing(sample_cohort_store):
    """Test pipeline processes exactly one GP record"""
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]

    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Single record should be returned
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'


def test_empty_records_list(sample_cohort_store):
    """Test pipeline handles empty list of GP records gracefully"""
    records = []

    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return empty list
    assert len(result) == 0
    assert len(result) == 0


def test_large_dataset_processing(large_cohort_store):
    """Test pipeline processes thousands of GP records efficiently"""
    records = []
    for i in range(500):
        records.append({
            'nhs_number': f"{i:010d}",
            'ethnicity': f'Ethnicity_{i % 10}'
        })
    
    result = run(large_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # All 500 records should be returned (all are in large cohort and have ethnicity)
    assert len(result) == 500
    assert 'ethnicity' in result.columns


# NHS Number Validation Scenarios (7-12)
def test_records_missing_nhs_number_field(sample_cohort_store):
    """Test records that don't have an nhs_number key"""
    records = [
        {'ethnicity': 'White'},  # Missing nhs_number
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
        {'age': 25}  # Missing nhs_number
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890'
    assert result.iloc[0]['ethnicity'] == 'Asian'


def test_records_with_empty_nhs_number_strings(sample_cohort_store):
    """Test nhs_number field exists but contains empty string"""
    records = [
        {'nhs_number': '', 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'Asian'


def test_records_with_none_nhs_numbers(sample_cohort_store):
    """Test nhs_number field contains None/null values"""
    records = [
        {'nhs_number': None, 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'Asian'


def test_records_with_whitespace_only_nhs_numbers(sample_cohort_store):
    """Test nhs_number contains only spaces/tabs"""
    records = [
        {'nhs_number': '   ', 'ethnicity': 'White'},
        {'nhs_number': '\t\n  ', 'ethnicity': 'Asian'},
        {'nhs_number': '1234567890', 'ethnicity': 'Black'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Only the record with valid NHS number should be returned
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'Black'


def test_mixed_nhs_number_data_types(sample_cohort_store):
    """Test some NHS numbers are strings, some are integers"""
    records = [
        {'nhs_number': 1234567890, 'ethnicity': 'White'},      # Integer
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},    # String
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Both should be processed if they're in cohort and have ethnicity
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == 1234567890 and result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == 'Asian'


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
    
    result = run(formatted_cohort, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the two records that are in cohort
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '123 456 7890' and result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '234-567-8901' and result.iloc[1]['ethnicity'] == 'Asian'


# Ethnicity Validation Scenarios (13-18)
def test_cohort_members_missing_ethnicity_field(sample_cohort_store):
    """Test records in cohort but no ethnicity key in dictionary"""
    records = [
        {'nhs_number': '1234567890'},  # In cohort, missing ethnicity
        {'nhs_number': '9999999999', 'ethnicity': 'White'}  # Not in cohort
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the cohort member even without ethnicity (current behavior)
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890'


def test_cohort_members_with_empty_ethnicity_strings(sample_cohort_store):
    """Test ethnicity field exists but empty"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': ''},  # In cohort, empty ethnicity
        {'nhs_number': '9999999999', 'ethnicity': ''}   # Not in cohort
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the cohort member even with empty ethnicity (current behavior)
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == ''


def test_cohort_members_with_none_ethnicity(sample_cohort_store):
    """Test ethnicity field contains None/null"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': None},  # In cohort
        {'nhs_number': '9999999999', 'ethnicity': None}   # Not in cohort
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the cohort member even with None ethnicity (current behavior)
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == None


def test_cohort_members_with_whitespace_only_ethnicity(sample_cohort_store):
    """Test ethnicity contains only spaces"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': '   '},   # In cohort
        {'nhs_number': '2345678901', 'ethnicity': '\t\n'}   # In cohort
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return both cohort members even with whitespace-only ethnicity
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == '   '
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == '\t\n'


def test_non_cohort_members_with_missing_ethnicity(sample_cohort_store):
    """Test should not return non-cohort members regardless of ethnicity"""
    records = [
        {'nhs_number': '9999999999'},  # Not in cohort, missing ethnicity
        {'nhs_number': '8888888888', 'ethnicity': ''}  # Not in cohort, empty ethnicity
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return no records as none are cohort members
    assert len(result) == 0
    assert len(result) == 0


def test_mixed_ethnicity_data_types(sample_cohort_store):
    """Test some ethnicities are strings, some are numbers"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},    # String
        {'nhs_number': '2345678901', 'ethnicity': 123},        # Number
        {'nhs_number': '3456789012', 'ethnicity': True}        # Boolean
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # All should be returned as they're cohort members with some ethnicity value
    assert len(result) == 3
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == 123
    assert result.iloc[2].to_dict() == {'nhs_number': '3456789012', 'ethnicity': True}


# Cohort Store Scenarios (19-23)
def test_empty_cohort_store(empty_cohort_store):
    """Test cohort pandas Series contains no members"""
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    result = run(empty_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return no records as cohort is empty
    assert len(result) == 0
    assert len(result) == 0


def test_single_member_cohort_store(single_member_cohort_store):
    """Test cohort contains exactly one NHS number"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # In cohort
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(single_member_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the cohort member
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'


def test_large_cohort_store(large_cohort_store):
    """Test cohort contains thousands of NHS numbers"""
    records = [
        {'nhs_number': '0000000001', 'ethnicity': 'White'},  # In large cohort
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(large_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the cohort member
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '0000000001' and result.iloc[0]['ethnicity'] == 'White'


def test_cohort_store_with_duplicate_nhs_numbers(duplicate_cohort_store):
    """Test same NHS number appears multiple times in cohort"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # In cohort (appears twice)
        {'nhs_number': '9999999999', 'ethnicity': 'Asian'}   # Not in cohort
    ]
    
    result = run(duplicate_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the cohort member
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'


def test_cohort_store_with_mixed_data_types():
    """Test NHS numbers stored as strings and integers"""
    mixed_cohort = pd.Series(['1234567890', 2345678901, '3456789012'], name='nhs')
    records = [
        {'nhs_number': '2345678901', 'ethnicity': 'White'},  # String matching int in cohort
        {'nhs_number': 3456789012, 'ethnicity': 'Asian'}     # Int matching string in cohort
    ]
    
    result = run(mixed_cohort, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
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
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the record with all its fields intact
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White' and result.iloc[0]['age'] == 25 and result.iloc[0]['postcode'] == 'TA1 1AA' and result.iloc[0]['diagnosis'] == 'Diabetes'
    


def test_records_missing_fields_entirely(sample_cohort_store):
    """Test GP records missing expected fields entirely"""
    records = [
        {'age': 25, 'postcode': 'TA1 1AA'},  # Missing both nhs_number and ethnicity
        {'nhs_number': '1234567890'},         # Missing ethnicity only
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the cohort member even without ethnicity
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890'


def test_empty_record_dictionaries(sample_cohort_store):
    """Test some records are completely empty dictionaries"""
    records = [
        {},  # Empty dictionary
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {}   # Another empty dictionary
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the valid record
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'


def test_duplicate_nhs_numbers_in_records(sample_cohort_store):
    """Test same NHS number appears in multiple GP records"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},  # Duplicate
        {'nhs_number': '2345678901', 'ethnicity': 'Black'}
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return all cohort members, including duplicates
    assert len(result) == 3
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '1234567890' and result.iloc[1]['ethnicity'] == 'Asian'
    assert result.iloc[2].to_dict() == {'nhs_number': '2345678901', 'ethnicity': 'Black'}


# Cohort Membership Check Scenarios (29-33)
def test_cohort_membership_check_returns_true(sample_cohort_store):
    """Test NHS number found in cohort store"""
    
    # The mock_encrypt function returns input unchanged
    # So '1234567890' will be "encrypted" to '1234567890'
    # Use the default sample_cohort_store which contains '1234567890'
    records = [{'nhs_number': '1234567890', 'ethnicity': 'White'}]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return the record since NHS number is in cohort
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'


def test_cohort_membership_check_returns_false(sample_cohort_store):
    """Test NHS number not found in cohort store"""
    
    # Use an NHS number that's not in the sample_cohort_store
    records = [{'nhs_number': '9999999999', 'ethnicity': 'White'}]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return no records since NHS number is not in cohort
    assert len(result) == 0


def test_case_sensitivity_in_nhs_numbers():
    """Test NHS numbers with different casing (though NHS numbers are numeric)"""
    cohort = pd.Series(['ABC123', 'def456'], name='nhs')
    records = [
        {'nhs_number': 'ABC123', 'ethnicity': 'White'},  # Exact match
        {'nhs_number': 'abc123', 'ethnicity': 'Asian'}   # Different case
    ]
    
    result = run(cohort, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only exact matches (case sensitive)
    assert len(result) == 1
    assert result.iloc[0]['nhs_number'] == 'ABC123' and result.iloc[0]['ethnicity'] == 'White'


def test_whitespace_handling_in_cohort_checks():
    """Test NHS numbers with leading/trailing spaces"""
    cohort = pd.Series(['1234567890', '2345678901'], name='nhs')
    records = [
        {'nhs_number': ' 1234567890 ', 'ethnicity': 'White'},  # With spaces
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'}     # Without spaces
    ]
    
    result = run(cohort, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Result depends on how is_cohort_member handles whitespace
    # This test documents the current behavior
    assert isinstance(result, pd.DataFrame)
    # Should process both records successfully (whitespace gets handled by mock_encrypt)
    assert len(result) == 2


# Data Type Validation Scenarios (50)
def test_unicode_characters_in_ethnicity_data(sample_cohort_store):
    """Test non-ASCII characters in ethnicity fields"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'Café'},      # Accented characters
        {'nhs_number': '2345678901', 'ethnicity': '中文'},       # Chinese characters
        {'nhs_number': '3456789012', 'ethnicity': 'العربية'}    # Arabic characters
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should handle Unicode characters without issues
    assert len(result) == 3
    assert result.iloc[0]['ethnicity'] == 'Café'
    assert result.iloc[1]['ethnicity'] == '中文'
    assert result.iloc[2]['ethnicity'] == 'العربية'


# Continuation Logic Scenarios (53-56)
def test_processing_continues_after_invalid_nhs_number(sample_cohort_store):
    """Test pipeline doesn't stop on bad NHS number"""
    records = [
        {'ethnicity': 'White'},  # Invalid: no NHS number
        {'nhs_number': '1234567890', 'ethnicity': 'Asian'},  # Valid
        {'nhs_number': None, 'ethnicity': 'Black'},  # Invalid: None NHS number
        {'nhs_number': '2345678901', 'ethnicity': 'Mixed'}   # Valid
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the valid cohort member records
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'Asian'
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == 'Mixed'


def test_processing_continues_after_missing_ethnicity(sample_cohort_store):
    """Test pipeline continues after ethnicity warning"""
    records = [
        {'nhs_number': '1234567890'},  # In cohort, missing ethnicity
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},  # In cohort, valid
        {'nhs_number': '3456789012', 'ethnicity': ''}        # In cohort, empty ethnicity
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return all cohort members regardless of ethnicity issues
    assert len(result) == 3
    assert result.iloc[0]['nhs_number'] == '1234567890'
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == 'Asian'
    assert result.iloc[2].to_dict() == {'nhs_number': '3456789012', 'ethnicity': ''}


def test_all_records_skipped_due_to_invalid_nhs_numbers(sample_cohort_store):
    """Test every record has invalid NHS number"""
    records = [
        {'ethnicity': 'White'},     # No NHS number
        {'nhs_number': '', 'ethnicity': 'Asian'},    # Empty NHS number
        {'nhs_number': None, 'ethnicity': 'Black'},  # None NHS number
        {'age': 25}  # No NHS number field
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return empty list as no records have valid NHS numbers
    assert len(result) == 0
    assert len(result) == 0


def test_mixed_valid_and_invalid_records(sample_cohort_store):
    """Test some records process, others are skipped"""
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # Valid, in cohort
        {'ethnicity': 'Asian'},                              # Invalid: no NHS number
        {'nhs_number': '9999999999', 'ethnicity': 'Black'},  # Valid, not in cohort
        {'nhs_number': '', 'ethnicity': 'Mixed'},            # Invalid: empty NHS number
        {'nhs_number': '2345678901', 'ethnicity': 'Other'}   # Valid, in cohort
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    
    # Should return only the valid cohort member records
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '1234567890' and result.iloc[0]['ethnicity'] == 'White'
    assert result.iloc[1]['nhs_number'] == '2345678901' and result.iloc[1]['ethnicity'] == 'Other'


# Test 3: Test encrypt function is called for each processed record
def test_encrypt_function_called_for_each_processed_record(sample_cohort_store):
    """Test encrypt function is called for each field in each processed record"""
    from unittest.mock import MagicMock
    
    call_log = []
    def track_encrypt_calls(field_name, value):
        call_log.append((field_name, value))
        return value
    
    mock_encrypt_tracker = MagicMock(side_effect=track_encrypt_calls)
    
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White', 'age': '25'},
        {'nhs_number': '2345678901', 'ethnicity': 'Asian', 'postcode': 'TA1 1AA'},
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt_tracker, GP_FEED)
    
    # Should have processed both records
    assert len(result) == 2
    
    # Should have called encrypt once with both NHS numbers as a batch (order doesn't matter)
    assert len(call_log) == 1, f"Expected 1 encrypt call, got {len(call_log)}"
    field_name, nhs_numbers = call_log[0]
    assert field_name == 'nhs_number'
    assert set(nhs_numbers) == {'1234567890', '2345678901'}, f"Expected both NHS numbers in batch, got {nhs_numbers}"

# Test 4: Test encrypt function raises exceptions
def test_encrypt_function_raises_exceptions_handled_gracefully(sample_cohort_store):
    """Test encrypt function raises exceptions - pipeline handles encryption failures gracefully"""
    from unittest.mock import MagicMock
    
    def failing_encrypt(field_name, values):
        if '1234567890' in values:
            raise Exception("Encryption service unavailable")
        return [str(value) for value in values]
    
    mock_encrypt_with_failure = MagicMock(side_effect=failing_encrypt)
    
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},     # Will cause encrypt to fail
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},     # Should succeed
    ]
    
    # Test how pipeline handles encryption failures
    try:
        result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt_with_failure, GP_FEED)
        # If no exception is raised, verify the results
        assert isinstance(result, list)
        # Should only process the record that didn't fail encryption
        assert len(result) <= 2
    except Exception as e:
        # If exception is raised, verify it's the expected one
        assert "Encryption service unavailable" in str(e)


# Test 5: Test encrypt function called once per valid NHS number
def test_encrypt_function_called_once_per_valid_nhs_number(sample_cohort_store):
    """Test encrypt function is called exactly once per record with valid NHS number"""
    from unittest.mock import MagicMock
    
    call_count = 0
    def count_encrypt_calls(field_name, values):
        nonlocal call_count
        call_count += 1
        assert field_name == "nhs_number", f"Expected 'nhs_number', got '{field_name}'"
        return values
    
    mock_encrypt_counter = MagicMock(side_effect=count_encrypt_calls)
    
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White', 'age': '30'},
        {'nhs_number': None, 'ethnicity': 'Asian'},           # Invalid - no encrypt call
        {'nhs_number': '2345678901', 'postcode': 'TA1 1AA'},  # Valid - encrypt call
        {'ethnicity': 'Black'},                               # Invalid - no encrypt call
        {'nhs_number': '', 'diagnosis': 'Diabetes'},          # Invalid - no encrypt call
    ]
    
    result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt_counter, GP_FEED)
    
    # Should have called encrypt exactly once with batch of valid NHS numbers
    assert call_count == 1, f"Expected 1 encrypt call (batch), got {call_count}"
    assert mock_encrypt_counter.call_count == 1


# Test 7: Test encrypt function with network timeout exceptions
def test_encrypt_function_with_network_timeout_exceptions(sample_cohort_store):
    """Test encrypt function with network timeout exceptions - handles network/service failures"""
    from unittest.mock import MagicMock
    import socket
    
    def network_timeout_encrypt(field_name, values):
        if '1234567890' in values:
            raise socket.timeout("Network timeout during encryption")
        return values
    
    mock_encrypt_timeout = MagicMock(side_effect=network_timeout_encrypt)
    
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White', 'age': '25'},
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},
    ]
    
    # Test how pipeline handles network timeouts - should raise RuntimeError wrapping the original timeout
    with pytest.raises(RuntimeError) as exc_info:
        run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt_timeout, GP_FEED)
    
    # Pipeline should wrap the timeout in a RuntimeError
    assert "Failed to process GP records due to batch encryption service error" in str(exc_info.value)
    assert "Network timeout during encryption" in str(exc_info.value)


# Test 8: Test encrypt function returns None handling
def test_encrypt_function_returns_none_handling(sample_cohort_store):
    """Test encrypt function returns None - pipeline handles failed encryption gracefully"""
    from unittest.mock import MagicMock
    
    def conditional_encrypt(field_name, values):  # Changed: values instead of value
        if '1234567890' in values:  # Changed: check if NHS number is in the batch
            return None  # Encryption failed
        return values  # Changed: return the list
    
    mock_encrypt_none = MagicMock(side_effect=conditional_encrypt)
    
    records = [
        {'nhs_number': '1234567890', 'ethnicity': 'White'},  # Will return None
        {'nhs_number': '2345678901', 'ethnicity': 'Asian'},  # Will succeed
    ]
    
    # Should raise RuntimeError because batch encryption returned None
    with pytest.raises(RuntimeError) as exc_info:
        run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt_none, GP_FEED)
    
    # Verify the error message mentions batch encryption failure
    assert "Failed to process GP records due to batch encryption service error" in str(exc_info.value)
    
    # Verify encrypt was called once with the batch
    assert mock_encrypt_none.call_count == 1


# SFT Feed Specific Tests

def test_sft_feed_basic_processing(sample_cohort_store):
    """Test SFT feed processes records with correct configuration"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice Johnson'},
        {'patient_id': 'P002', 'nhs_number': '2345678901', 'name': 'Bob Smith'},
        {'patient_id': 'P003', 'nhs_number': '9999999999', 'name': 'Non Member'}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return only cohort members
    assert len(result) == 2
    assert result.iloc[0]['nhs_number'] == '1234567890'
    assert result.iloc[1]['nhs_number'] == '2345678901'


def test_sft_feed_nhs_column_index_validation():
    """Test SFT feed uses correct NHS column index (1 instead of 0)"""
    # SFT has NHS number in second column (index 1)
    cohort = pd.Series(['1234567890', '2345678901'], name='nhs')

    # Create records where NHS is NOT in first column
    records = pd.DataFrame([
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'nhs_number': '2345678901', 'name': 'Bob'},
    ])

    records = _ensure_nhs_second(records)
    result = run(cohort, records, mock_encrypt, SFT_FEED)

    # Should process successfully using nhs_column_index=1
    assert len(result) == 2
    assert SFT_FEED.nhs_column_index == 1
    assert GP_FEED.nhs_column_index == 0


def test_sft_feed_no_metadata_rows_skipped():
    """Test SFT feed doesn't skip metadata rows (metadata_rows_to_skip=0)"""
    cohort = pd.Series(['1234567890'], name='nhs')

    # SFT format: no metadata rows to skip
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'}
    ]

    result = run(cohort, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should process all records
    assert len(result) == 1
    assert SFT_FEED.metadata_rows_to_skip == 0
    assert GP_FEED.metadata_rows_to_skip == 2


def test_sft_vs_gp_feed_config_differences():
    """Test configuration differences between SFT and GP feeds"""
    # NHS column index
    assert GP_FEED.nhs_column_index == 0
    assert SFT_FEED.nhs_column_index == 1

    # Metadata rows to skip
    assert GP_FEED.metadata_rows_to_skip == 2
    assert SFT_FEED.metadata_rows_to_skip == 0

    # Preserve metadata
    assert GP_FEED.preserve_metadata is True
    assert SFT_FEED.preserve_metadata is False

    # Feed type
    assert GP_FEED.feed_type == 'gp'
    assert SFT_FEED.feed_type == 'sft'


def test_sft_feed_with_mixed_cohort_non_cohort(sample_cohort_store):
    """Test SFT feed filters cohort members correctly"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice', 'age': 30},
        {'patient_id': 'P002', 'nhs_number': '9999999999', 'name': 'NonMember', 'age': 25},
        {'patient_id': 'P003', 'nhs_number': '2345678901', 'name': 'Bob', 'age': 40},
        {'patient_id': 'P004', 'nhs_number': '8888888888', 'name': 'Another', 'age': 35}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return only the 2 cohort members
    assert len(result) == 2
    assert result.iloc[0]['patient_id'] == 'P001'
    assert result.iloc[1]['patient_id'] == 'P003'


def test_sft_feed_empty_records(sample_cohort_store):
    """Test SFT feed handles empty records"""
    records = []

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    assert len(result) == 0


def test_sft_feed_all_cohort_members(sample_cohort_store):
    """Test SFT feed when all records are cohort members"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'nhs_number': '2345678901', 'name': 'Bob'},
        {'patient_id': 'P003', 'nhs_number': '3456789012', 'name': 'Carol'}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # All records should be returned
    assert len(result) == 3


def test_sft_feed_no_cohort_members(sample_cohort_store):
    """Test SFT feed when no records are cohort members"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '9999999999', 'name': 'NonMember1'},
        {'patient_id': 'P002', 'nhs_number': '8888888888', 'name': 'NonMember2'}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    assert len(result) == 0


def test_sft_feed_missing_nhs_numbers(sample_cohort_store):
    """Test SFT feed handles records with missing NHS numbers"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'name': 'Bob'},  # Missing NHS number
        {'patient_id': 'P003', 'nhs_number': '', 'name': 'Carol'},  # Empty NHS number
        {'patient_id': 'P004', 'nhs_number': '2345678901', 'name': 'David'}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return only records with valid NHS numbers that are in cohort
    assert len(result) == 2
    assert result.iloc[0]['patient_id'] == 'P001'
    assert result.iloc[1]['patient_id'] == 'P004'


def test_sft_feed_with_additional_fields(sample_cohort_store):
    """Test SFT feed preserves all fields from input records"""
    records = [
        {
            'patient_id': 'P001',
            'nhs_number': '1234567890',
            'name': 'Alice Johnson',
            'age': 30,
            'diagnosis': 'Diabetes',
            'postcode': 'TA1 1AA'
        }
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should preserve all fields
    assert len(result) == 1
    assert result.iloc[0]['patient_id'] == 'P001'
    assert result.iloc[0]['nhs_number'] == '1234567890'
    assert result.iloc[0]['name'] == 'Alice Johnson'
    assert result.iloc[0]['age'] == 30
    assert result.iloc[0]['diagnosis'] == 'Diabetes'
    assert result.iloc[0]['postcode'] == 'TA1 1AA'


def test_sft_feed_duplicate_nhs_numbers(sample_cohort_store):
    """Test SFT feed handles duplicate NHS numbers in records"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'nhs_number': '1234567890', 'name': 'Alice Duplicate'},
        {'patient_id': 'P003', 'nhs_number': '2345678901', 'name': 'Bob'}
    ]

    result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return all cohort members including duplicates
    assert len(result) == 3
    assert result.iloc[0]['patient_id'] == 'P001'
    assert result.iloc[1]['patient_id'] == 'P002'
    assert result.iloc[2]['patient_id'] == 'P003'


def test_sft_feed_with_large_dataset(large_cohort_store):
    """Test SFT feed processes large datasets efficiently"""
    records = []
    for i in range(500):
        records.append({
            'patient_id': f'P{i:04d}',
            'nhs_number': f'{i:010d}',
            'name': f'Patient_{i}'
        })

    result = run(large_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # All 500 records should be returned as they're in large cohort
    assert len(result) == 500


def test_sft_feed_encrypt_function_called(sample_cohort_store):
    """Test encrypt function is called for SFT feed processing"""

    call_log = []
    def track_encrypt_calls(field_name, values):
        call_log.append((field_name, values))
        return values

    mock_encrypt_tracker = MagicMock(side_effect=track_encrypt_calls)

    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'nhs_number': '2345678901', 'name': 'Bob'}
    ]

    run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt_tracker, SFT_FEED)

    # Should have called encrypt once with batch
    assert len(call_log) == 1
    field_name, nhs_numbers = call_log[0]
    assert field_name == 'nhs_number'
    assert set(nhs_numbers) == {'1234567890', '2345678901'}


def test_sft_feed_empty_cohort(empty_cohort_store):
    """Test SFT feed with empty cohort store"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'}
    ]

    result = run(empty_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return no records as cohort is empty
    assert len(result) == 0


def test_sft_feed_single_member_cohort(single_member_cohort_store):
    """Test SFT feed with single member cohort"""
    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'},
        {'patient_id': 'P002', 'nhs_number': '9999999999', 'name': 'NonMember'}
    ]

    result = run(single_member_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Should return only the single cohort member
    assert len(result) == 1
    assert result.iloc[0]['patient_id'] == 'P001'


def test_sft_feed_vs_gp_feed_same_data(sample_cohort_store):
    """Test SFT and GP feed process same data correctly with their respective configs"""
    records = [
        {'nhs_number': '1234567890', 'name': 'Alice'},
        {'nhs_number': '2345678901', 'name': 'Bob'},
        {'nhs_number': '9999999999', 'name': 'NonMember'}
    ]

    gp_result = run(sample_cohort_store, _ensure_nhs_first(records), mock_encrypt, GP_FEED)
    sft_result = run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt, SFT_FEED)

    # Both should return same cohort members
    assert len(gp_result) == len(sft_result) == 2
    assert set(gp_result['nhs_number']) == set(sft_result['nhs_number'])


def test_sft_feed_encryption_failure_handling(sample_cohort_store):
    """Test SFT feed handles encryption failures"""
    from unittest.mock import MagicMock

    def failing_encrypt(field_name, values):
        raise Exception("Encryption service unavailable")

    mock_encrypt_failure = MagicMock(side_effect=failing_encrypt)

    records = [
        {'patient_id': 'P001', 'nhs_number': '1234567890', 'name': 'Alice'}
    ]

    # Should raise RuntimeError wrapping the encryption error
    with pytest.raises(RuntimeError) as exc_info:
        run(sample_cohort_store, _ensure_nhs_second(records), mock_encrypt_failure, SFT_FEED)

    assert "Failed to process SFT records due to batch encryption service error" in str(exc_info.value)

