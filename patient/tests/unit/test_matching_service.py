"""
Tests for LinkageService.
"""
import pytest
import pandas as pd
from datetime import date
from unittest.mock import Mock, patch
from matching.service import MatchingService
from matching.patient import Sex


@pytest.fixture
def mock_repository():
    """Create a mock PatientRepository."""
    return Mock()


@pytest.fixture
def service(mock_repository):
    """Create LinkageService with mocked repository."""
    return MatchingService(mock_repository)


# ============================================
# Core Functionality (Matching & Creation)
# ============================================

def test_all_rows_match_in_local_mpi(service, mock_repository):
    """Test when all rows match existing patients in local MPI."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
    ])
    
    # Mock repository to return matches for all rows
    mock_repository.find_patients.return_value = [['patient-1'], ['patient-2']]
    
    result = service.match(df)
    
    # All rows should have patient_ids
    assert result.loc[0, 'patient_ids'] == ['patient-1']
    assert result.loc[1, 'patient_ids'] == ['patient-2']
    
    # Repository methods called correctly
    mock_repository.find_patients.assert_called_once()
    mock_repository.save.assert_not_called()  # No new patients created


def test_no_rows_match_in_local_mpi(service, mock_repository):
    """Test when no rows match existing patients - all get new unverified patients."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
    ])
    
    # Mock repository to return empty lists (no matches)
    mock_repository.find_patients.return_value = [[], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2']
    
    result = service.match(df)
    
    # All rows should have patient_ids from newly created patients
    assert result.loc[0, 'patient_ids'] == ['new-patient-1']
    assert result.loc[1, 'patient_ids'] == ['new-patient-2']
    
    # Repository methods called correctly
    mock_repository.find_patients.assert_called_once()
    mock_repository.save.assert_called_once()
    
    # Verify unverified patients have verified=False
    saved_df = mock_repository.save.call_args[0][0]
    assert all(saved_df['verified'] == False)

def test_mixed_some_match_some_dont(service, mock_repository):
    """Test mixed scenario: some rows match, others need new unverified patients."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'},
        {'nhs_number': '9434765828', 'dob': "1990-07-10", 'first_name': 'Bob', 'last_name': 'Jones', 'postcode': 'M1 1AE', 'sex': 'male'}
    ])
    
    # Mock repository: first row matches, second and third don't
    mock_repository.find_patients.return_value = [['existing-patient-1'], [], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2']
    
    result = service.match(df)
    
    # First row has existing patient, others have new patients
    assert result.loc[0, 'patient_ids'] == ['existing-patient-1']
    assert result.loc[1, 'patient_ids'] == ['new-patient-1']
    assert result.loc[2, 'patient_ids'] == ['new-patient-2']
    
    # Save only called for unmatched rows
    assert mock_repository.save.call_count == 1
    saved_df = mock_repository.save.call_args[0][0]
    assert len(saved_df) == 2  # Only 2 unmatched rows


def test_multiple_matches_per_row(service, mock_repository):
    """Test when a row matches multiple patients in local MPI."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
    ])
    
    # Mock repository to return multiple matches
    mock_repository.find_patients.return_value = [['patient-1', 'patient-2', 'patient-3']]
    
    result = service.match(df)
    
    # Row should have list with multiple patient IDs
    assert result.loc[0, 'patient_ids'] == ['patient-1', 'patient-2', 'patient-3']
    assert len(result.loc[0, 'patient_ids']) == 3


# ============================================
# Searchable Data Filtering
# ============================================

def test_all_rows_have_no_searchable_data(service, mock_repository):
    """Test when all rows have no valid searchable data after cleaning."""
    df = pd.DataFrame([
        {'nhs_number': '1234567890', 'dob': None, 'first_name': None, 'last_name': None, 'postcode': 'INVALID', 'sex': None},  # Invalid NHS
        {'nhs_number': None, 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None}  # All None
    ])
    
    result = service.match(df)
    
    # All rows should have empty list for patient_ids (unsearchable)
    assert result.loc[0, 'patient_ids'] == []
    assert result.loc[1, 'patient_ids'] == []
    
    # No database queries or saves
    mock_repository.find_patients.assert_not_called()
    mock_repository.save.assert_not_called()

def test_some_rows_have_no_searchable_data(service, mock_repository):
    """Test mixed scenario: some rows match NHS trace, some match cross-check trace, others not searchable."""
    df = pd.DataFrame([
        # row 0 - 1 unverified patient: NHS trace: valid NHS and dob
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},
        # row 1 - 0 unverified patient: NHS number present, dob missing (should be unsearchable)
        {'nhs_number': '9434765919', 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},
        # row 2 - 1 unverified patient: Cross-check trace: all fields except NHS
        {'nhs_number': None, 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        # row 3 - 1 unverified patient: Both traces valid (all fields present)
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'},
        # row 4 - 0 unverified patient: Neither trace valid (all fields missing)
        {'nhs_number': None, 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},
        # row 5 - 1 unverified patient: Invalid NHS but still a valid cross-check 
        {'nhs_number': '1234567890', 'dob': "1990-01-01", 'first_name': 'Alice', 'last_name': 'Brown', 'postcode': 'M1 1AE', 'sex': 'female'},
        # row 6 - 1 unverified patient: Valid NHS, valid cross-check (all required for cross-check present)
        {'nhs_number': '9434765828', 'dob': "1990-07-10", 'first_name': 'Bob', 'last_name': 'Jones', 'postcode': 'M1 1AE', 'sex': 'male'},
        # row 7 - 1 unverified patient: NHS too short but still a valid cross-check 
        {'nhs_number': '12345', 'dob': "1985-12-12", 'first_name': 'Bob', 'last_name': 'Smith', 'postcode': 'E2 8AA', 'sex': 'male'},
        # row 8 - 0 unverified patient: All fields present but invalid formats (should be unsearchable)
        {'nhs_number': 'invalid', 'dob': 'not-a-date', 'first_name': '', 'last_name': '', 'postcode': 'bad', 'sex': ''},
    ])
    
    # Only truly searchable rows should be processed
    mock_repository.find_patients.return_value = [[], [], [], [], [], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2', 'new-patient-3', 'new-patient-4', 'new-patient-5', 'new-patient-6']
    
    result = service.match(df)
    
    # NHS trace (valid NHS and dob)
    assert result.loc[0, 'patient_ids'] == ['new-patient-1']
    # NHS number present, dob missing (should be unsearchable)
    assert result.loc[1, 'patient_ids'] == []
    # Cross-check trace
    assert result.loc[2, 'patient_ids'] == ['new-patient-2']
    # Both traces valid
    assert result.loc[3, 'patient_ids'] == ['new-patient-3']
    # Neither trace valid
    assert result.loc[4, 'patient_ids'] == []
    # Invalid NHS but still a valid cross-check
    assert result.loc[5, 'patient_ids'] == ['new-patient-4']
    # Valid NHS, valid cross-check
    assert result.loc[6, 'patient_ids'] == ['new-patient-5']
    # NHS too short, but still a valid cross-check
    assert result.loc[7, 'patient_ids'] == ['new-patient-6']
    # All fields invalid
    assert result.loc[8, 'patient_ids'] == []

    # Only truly searchable rows processed
    assert mock_repository.find_patients.call_count == 1
    searchable_df = mock_repository.find_patients.call_args[0][0]
    assert len(searchable_df) == 6  # Only 6 rows are truly searchable

def test_row_becomes_unsearchable_after_cleaning(service, mock_repository):
    """Test row with invalid data becomes unsearchable after validation."""
    df = pd.DataFrame([
        {'nhs_number': '1234567890', 'dob': "2050-01-01", 'first_name': '', 'last_name': '   ', 'postcode': 'INVALID', 'sex': ''},
    ])
    
    result = service.match(df)
    
    # Row should have empty list (all fields invalid)
    assert result.loc[0, 'patient_ids'] == []
    
    # No database operations
    mock_repository.find_patients.assert_not_called()
    mock_repository.save.assert_not_called()


# ============================================
# DataFrame Handling
# ============================================

def test_empty_dataframe_raises_error(service):
    """Test that empty DataFrame raises ValueError."""
    df = pd.DataFrame(columns=['nhs_number', 'dob', 'first_name', 'last_name', 'postcode', 'sex'])
    
    with pytest.raises(ValueError, match="DataFrame is empty"):
        service.match(df)


def test_single_row_dataframe(service, mock_repository):
    """Test processing of single row DataFrame."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
    ])
    
    mock_repository.find_patients.return_value = [['patient-1']]
    
    result = service.match(df)
    
    assert len(result) == 1
    assert result.loc[0, 'patient_ids'] == ['patient-1']


# ============================================
# Data Integrity
# ============================================

def test_index_preservation(service, mock_repository):
    """Test that original DataFrame indices are preserved."""
    # Create DataFrame with non-default index
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
    ], index=[10, 25])
    
    mock_repository.find_patients.return_value = [['patient-1'], ['patient-2']]
    
    result = service.match(df)
    
    # Original indices preserved
    assert list(result.index) == [10, 25]
    assert result.loc[10, 'patient_ids'] == ['patient-1']
    assert result.loc[25, 'patient_ids'] == ['patient-2']


def test_original_dataframe_not_mutated(service, mock_repository):
    """Test that input DataFrame is not modified."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'john', 'last_name': 'doe', 'postcode': 'sw1a1aa', 'sex': 'male'}
    ])
    
    # Store original values
    original_first_name = df.loc[0, 'first_name']
    original_postcode = df.loc[0, 'postcode']
    original_columns = list(df.columns)
    
    mock_repository.find_patients.return_value = [['patient-1']]
    
    result = service.match(df)
    
    # Original DataFrame unchanged
    assert df.loc[0, 'first_name'] == original_first_name
    assert df.loc[0, 'postcode'] == original_postcode
    assert 'patient_ids' not in df.columns
    assert list(df.columns) == original_columns
    
    # Result DataFrame has changes
    assert result.loc[0, 'first_name'] == 'John'  # Title-cased
    assert result.loc[0, 'postcode'] == 'SW1A 1AA'  # Formatted
    assert 'patient_ids' in result.columns


def test_patient_ids_column_format_consistency(service, mock_repository):
    """Test that patient_ids column has consistent format (empty list or List[str])."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},  # Will match
        {'nhs_number': None, 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},  # Unsearchable
        {'nhs_number': '9434765870', 'dob': "1975-03-20", 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}  # Will not match
    ])
    
    mock_repository.find_patients.return_value = [['patient-1'], []]
    mock_repository.save.return_value = ['new-patient-1']
    
    result = service.match(df)
    
    # Check format consistency - all should be lists now
    assert isinstance(result.loc[0, 'patient_ids'], list)  # Matched - list
    assert result.loc[1, 'patient_ids'] == []  # Unsearchable - empty list
    assert isinstance(result.loc[2, 'patient_ids'], list)  # New patient - list


# ============================================
# Edge Cases
# ============================================

def test_all_fields_valid_after_cleaning(service, mock_repository):
    """Test row with all valid fields is processed correctly."""
    df = pd.DataFrame([
        {'nhs_number': '943 476 5919', 'dob': "1980-05-15", 'first_name': 'john', 'last_name': 'doe', 'postcode': 'sw1a1aa', 'sex': 'MALE'}
    ])
    
    mock_repository.find_patients.return_value = [[]]
    mock_repository.save.return_value = ['new-patient-1']
    
    result = service.match(df)
    
    # Check standardization happened
    assert result.loc[0, 'nhs_number'] == '9434765919'
    assert result.loc[0, 'first_name'] == 'John'
    assert result.loc[0, 'last_name'] == 'Doe'
    assert result.loc[0, 'postcode'] == 'SW1A 1AA'
    assert result.loc[0, 'sex'] == 'male'
    assert result.loc[0, 'patient_ids'] == ['new-patient-1']

def test_all_fields_invalid_after_cleaning(service, mock_repository):
    """Test row where all fields become None after validation."""
    df = pd.DataFrame([
        {'nhs_number': '1234567890', 'dob': "2050-01-01", 'first_name': '', 'last_name': '', 'postcode': 'INVALID', 'sex': ''}
    ])
    
    result = service.match(df)
    
    # All fields should be None after cleaning
    assert result.loc[0, 'nhs_number'] is None
    assert result.loc[0, 'dob'] is None
    assert result.loc[0, 'first_name'] is None
    assert result.loc[0, 'last_name'] is None
    assert result.loc[0, 'postcode'] is None
    assert result.loc[0, 'sex'] is None
    assert result.loc[0, 'patient_ids'] == []  # Unsearchable gets empty list
    
    # No database operations
    mock_repository.find_patients.assert_not_called()
    mock_repository.save.assert_not_called()


def test_duplicate_rows(service, mock_repository):
    """Test duplicate rows are processed independently with same results."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765919', 'dob': "1980-05-15", 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
    ])
    
    # Mock repository returns same match for both duplicate rows
    mock_repository.find_patients.return_value = [['patient-1'], ['patient-1']]
    
    result = service.match(df)
    
    # Both rows should have same patient_ids
    assert result.loc[0, 'patient_ids'] == ['patient-1']
    assert result.loc[1, 'patient_ids'] == ['patient-1']