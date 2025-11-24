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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
    ])
    
    # Mock repository to return empty lists (no matches)
    mock_repository.find_patients.return_value = [[], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2']
    
    with patch('matching.service.add_to_batch') as mock_add_to_batch:
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'},
        {'nhs_number': '9434765828', 'dob': date(1990, 7, 10), 'first_name': 'Bob', 'last_name': 'Jones', 'postcode': 'M1 1AE', 'sex': 'male'}
    ])
    
    # Mock repository: first row matches, second and third don't
    mock_repository.find_patients.return_value = [['existing-patient-1'], [], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2']
    
    with patch('matching.service.add_to_batch'):
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
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
    """Test mixed scenario: some rows searchable, others not."""
    df = pd.DataFrame([
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},  # Valid
        {'nhs_number': None, 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},  # All None
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}  # Valid
    ])
    
    # Mock repository: both searchable rows don't match (empty lists)
    mock_repository.find_patients.return_value = [[], []]
    mock_repository.save.return_value = ['new-patient-1', 'new-patient-2']
    
    with patch('matching.service.add_to_batch'):
        result = service.match(df)
    
    # Searchable rows get new patients, unsearchable row gets empty list
    assert result.loc[0, 'patient_ids'] == ['new-patient-1']
    assert result.loc[1, 'patient_ids'] == []
    assert result.loc[2, 'patient_ids'] == ['new-patient-2']
    
    # Only searchable rows processed
    assert mock_repository.find_patients.call_count == 1
    searchable_df = mock_repository.find_patients.call_args[0][0]
    assert len(searchable_df) == 2  # Only 2 searchable rows


def test_row_becomes_unsearchable_after_cleaning(service, mock_repository):
    """Test row with invalid data becomes unsearchable after validation."""
    df = pd.DataFrame([
        {'nhs_number': '1234567890', 'dob': date(2050, 1, 1), 'first_name': '', 'last_name': '   ', 'postcode': 'INVALID', 'sex': ''},
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'john', 'last_name': 'doe', 'postcode': 'sw1a1aa', 'sex': 'male'}
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},  # Will match
        {'nhs_number': None, 'dob': None, 'first_name': None, 'last_name': None, 'postcode': None, 'sex': None},  # Unsearchable
        {'nhs_number': '9434765870', 'dob': date(1975, 3, 20), 'first_name': 'Jane', 'last_name': 'Smith', 'postcode': 'E1 6AN', 'sex': 'female'}  # Will not match
    ])
    
    mock_repository.find_patients.return_value = [['patient-1'], []]
    mock_repository.save.return_value = ['new-patient-1']
    
    with patch('matching.service.add_to_batch'):
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
        {'nhs_number': '943 476 5919', 'dob': date(1980, 5, 15), 'first_name': 'john', 'last_name': 'doe', 'postcode': 'sw1a1aa', 'sex': 'MALE'}
    ])
    
    mock_repository.find_patients.return_value = [[]]
    mock_repository.save.return_value = ['new-patient-1']
    
    with patch('matching.service.add_to_batch'):
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
        {'nhs_number': '1234567890', 'dob': date(2050, 1, 1), 'first_name': '', 'last_name': '', 'postcode': 'INVALID', 'sex': ''}
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
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'},
        {'nhs_number': '9434765919', 'dob': date(1980, 5, 15), 'first_name': 'John', 'last_name': 'Doe', 'postcode': 'SW1A 1AA', 'sex': 'male'}
    ])
    
    # Mock repository returns same match for both duplicate rows
    mock_repository.find_patients.return_value = [['patient-1'], ['patient-1']]
    
    result = service.match(df)
    
    # Both rows should have same patient_ids
    assert result.loc[0, 'patient_ids'] == ['patient-1']
    assert result.loc[1, 'patient_ids'] == ['patient-1']