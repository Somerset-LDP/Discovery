import pytest
import pandas as pd
from datetime import date
from unittest.mock import Mock, MagicMock
from mpi.local.matching import SqlExactMatchStrategy


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = Mock()
    mock_connection = MagicMock()
    engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
    engine.connect.return_value.__exit__ = Mock(return_value=None)
    return engine, mock_connection


@pytest.fixture
def strategy(mock_engine):
    """Create SqlExactMatchStrategy with mocked engine."""
    engine, _ = mock_engine
    return SqlExactMatchStrategy(engine)


# ============================================
# Tests for query parameter extraction 
# ============================================

def test_empty_dataframe_returns_empty_list(strategy, mock_engine):
    """Empty DataFrame should return empty list without calling database."""
    _, mock_connection = mock_engine
    
    df = pd.DataFrame(columns=['nhs_number', 'dob', 'postcode', 'first_name', 'last_name', 'sex'])
    
    result = strategy.find_matches(df)
    
    assert result == []
    mock_connection.execute.assert_not_called()


def test_extracts_columns_correctly(strategy, mock_engine):
    """Verifies columns are extracted as lists with correct values."""
    _, mock_connection = mock_engine
    
    # Setup mock to return empty results
    mock_result = Mock()
    mock_result.fetchall.return_value = []
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([{
        'nhs_number': '1234567890',
        'dob': date(2000, 12, 25),
        'postcode': 'SW1A 1AA',
        'first_name': 'Test',
        'last_name': 'User',
        'sex': 'male'
    }])
    
    strategy.find_matches(df)
    
    # Verify execute was called with correct parameters
    call_args = mock_connection.execute.call_args
    params = call_args[0][1]  # Second argument to execute()
    
    assert params['row_indices'] == [0]
    assert params['nhs_numbers'] == ['1234567890']
    assert params['dobs'] == ['2000-12-25']  # Date converted to string
    assert params['postcodes'] == ['SW1A 1AA']
    assert params['first_names'] == ['Test']
    assert params['last_names'] == ['User']
    assert params['sexes'] == ['male']


def test_handles_none_values_in_columns(strategy, mock_engine):
    """Verifies None values are preserved in column extraction."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = []
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([{
        'nhs_number': '1234567890',
        'dob': None,
        'postcode': None,
        'first_name': 'John',
        'last_name': None,
        'sex': 'male'
    }])
    
    strategy.find_matches(df)
    
    call_args = mock_connection.execute.call_args
    params = call_args[0][1]
    
    assert params['nhs_numbers'] == ['1234567890']
    assert params['dobs'] == ['None']  # None converted to string 'None' by astype(str)
    assert params['postcodes'] == [None]
    assert params['first_names'] == ['John']
    assert params['last_names'] == [None]
    assert params['sexes'] == ['male']


def test_multiple_rows_indexed_correctly(strategy, mock_engine):
    """Verifies row indices are generated correctly for multiple rows."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = []
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([
        {'nhs_number': '1111111111', 'dob': date(1980, 1, 1), 'postcode': 'A1 1AA',
         'first_name': 'Alice', 'last_name': 'Smith', 'sex': 'female'},
        {'nhs_number': '2222222222', 'dob': date(1985, 2, 2), 'postcode': 'B2 2BB',
         'first_name': 'Bob', 'last_name': 'Jones', 'sex': 'male'},
        {'nhs_number': '3333333333', 'dob': date(1990, 3, 3), 'postcode': 'C3 3CC',
         'first_name': 'Charlie', 'last_name': 'Brown', 'sex': 'male'}
    ])
    
    strategy.find_matches(df)
    
    call_args = mock_connection.execute.call_args
    params = call_args[0][1]
    
    assert params['row_indices'] == [0, 1, 2]
    assert len(params['nhs_numbers']) == 3
    assert len(params['dobs']) == 3


# ============================================
# Tests for result grouping logic 
# ============================================

def test_single_match_grouped_correctly(strategy, mock_engine):
    """Single match for row 0 should return [['pat_123']]."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [(0, 'pat_123')]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([{
        'nhs_number': '1234567890',
        'dob': date(1980, 1, 15),
        'postcode': 'SW1A 1AA',
        'first_name': 'John',
        'last_name': 'Smith',
        'sex': 'male'
    }])
    
    result = strategy.find_matches(df)
    
    assert result == [['pat_123']]


def test_multiple_matches_grouped_correctly(strategy, mock_engine):
    """Multiple matches for same row should be grouped into single list."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [
        (0, 'pat_123'),
        (0, 'pat_456'),
        (0, 'pat_789')
    ]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([{
        'nhs_number': None,
        'dob': date(1990, 3, 10),
        'postcode': None,
        'first_name': 'Alex',
        'last_name': 'Johnson',
        'sex': 'other'
    }])
    
    result = strategy.find_matches(df)
    
    assert result == [['pat_123', 'pat_456', 'pat_789']]

def test_no_match_returns_empty_list(strategy, mock_engine):
    """Row with no matches (NULL patient_id from LEFT JOIN) should return empty list."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [(0, None)]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([{
        'nhs_number': '9999999999',
        'dob': date(1980, 1, 15),
        'postcode': 'XX1 1XX',
        'first_name': 'Unknown',
        'last_name': 'Person',
        'sex': 'male'
    }])
    
    result = strategy.find_matches(df)
    
    assert result == [[]]

def test_mixed_results_grouped_correctly(strategy, mock_engine):
    """Mix of single match, no match, and multiple matches should be grouped correctly."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [
        (0, 'pat_111'),      # Row 0: single match
        (1, None),           # Row 1: no match
        (2, 'pat_222'),      # Row 2: multiple matches
        (2, 'pat_333')
    ]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([
        {'nhs_number': '1111111111', 'dob': date(1980, 1, 1), 'postcode': 'A1 1AA',
         'first_name': 'Alice', 'last_name': 'Smith', 'sex': 'female'},
        {'nhs_number': '2222222222', 'dob': date(1985, 2, 2), 'postcode': 'B2 2BB',
         'first_name': 'Bob', 'last_name': 'Jones', 'sex': 'male'},
        {'nhs_number': None, 'dob': date(1990, 3, 3), 'postcode': 'C3 3CC',
         'first_name': 'Charlie', 'last_name': 'Brown', 'sex': 'male'}
    ])
    
    result = strategy.find_matches(df)
    
    assert result == [['pat_111'], [], ['pat_222', 'pat_333']]


def test_preserves_result_order(strategy, mock_engine):
    """Results should be returned in same order as input rows."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [
        (2, 'pat_333'),
        (0, 'pat_111'),
        (1, 'pat_222')
    ]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([
        {'nhs_number': '1', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None},
        {'nhs_number': '2', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None},
        {'nhs_number': '3', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None}
    ])
    
    result = strategy.find_matches(df)
    
    assert result == [['pat_111'], ['pat_222'], ['pat_333']]


def test_all_rows_no_matches(strategy, mock_engine):
    """All rows with no matches should return all None."""
    _, mock_connection = mock_engine
    
    mock_result = Mock()
    mock_result.fetchall.return_value = [(0, None), (1, None), (2, None)]
    mock_connection.execute.return_value = mock_result
    
    df = pd.DataFrame([
        {'nhs_number': '1', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None},
        {'nhs_number': '2', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None},
        {'nhs_number': '3', 'dob': None, 'postcode': None, 'first_name': None, 'last_name': None, 'sex': None}
    ])
    
    result = strategy.find_matches(df)
    
    assert result == [[], [], []]