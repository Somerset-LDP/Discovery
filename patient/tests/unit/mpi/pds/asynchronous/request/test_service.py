import pytest
from unittest.mock import MagicMock
from datetime import datetime
import pandas as pd

from mpi.pds.asynchronous.request.service import PdsAsyncRequestService

@pytest.fixture
def service():
    trace_status = MagicMock()
    mpi = MagicMock()
    return PdsAsyncRequestService(trace_status, mpi)

def test_no_unverified_patients(service):
    service.mpi.find_unverified_patients.return_value = pd.DataFrame(columns=["patient_id", "nhs_number", "date_of_birth", "postcode", "family_name", "given_name", "sex"])
    service.trace_status.find_untraced_patients.return_value = []
    result = service.submit()
    assert result["patient_ids"] == []
    assert result["submission_time"] is None

def test_all_patients_already_traced(service):
    df = pd.DataFrame({"patient_id": [1, 2, 3]})
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = []
    result = service.submit()
    assert result["patient_ids"] == []
    assert result["submission_time"] is None

def test_some_unverified_some_untraced(service):
    df = pd.DataFrame({
        "patient_id": [1, 2, 3],
        "nhs_number": ["a", "b", "c"],
        "family_name": ["Smith", "Jones", "Brown"],
        "given_name": ["John", "Jane", "Jim"],
        "sex": ["M", "F", "M"],
        "date_of_birth": ["2000-01-01", "1990-02-02", "1980-03-03"],
        "postcode": ["AB1", "BC2", "CD3"]
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [2, 3]
    result = service.submit()
    assert set(result["patient_ids"]) == {2, 3}
    assert result["submission_time"] is not None

def test_duplicate_patient_ids(service):
    df = pd.DataFrame({
        "patient_id": [1, 1, 2, 3, 3, 4],
        "nhs_number": ["a", "a", "b", "c", "c", "d"],
        "family_name": ["Smith", "Smith", "Jones", "Brown", "Brown", "White"],
        "given_name": ["John", "John", "Jane", "Jim", "Jim", "Sue"],
        "sex": ["M", "M", "F", "M", "M", "F"],
        "date_of_birth": ["2000-01-01", "2000-01-01", "1990-02-02", "1980-03-03", "1980-03-03", "1970-04-04"],
        "postcode": ["AB1", "AB1", "BC2", "CD3", "CD3", "DE4"]
    })
    # All patient IDs are untraced
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [1, 2, 3, 4]
    result = service.submit()
    # Only patient_id 2 and 4 should remain (no duplicates)
    assert set(result["patient_ids"]) == {2, 4}
    assert result["submission_time"] is not None

def test_missing_required_patient_fields(service):
    # 1. Missing 'sex', 'date_of_birth', 'postcode' (should be excluded)
    df = pd.DataFrame({
        "patient_id": [1],
        "nhs_number": ["a"],
        "family_name": ["Smith"],
        "given_name": ["John"],
        # missing 'sex', 'date_of_birth', 'postcode'
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [1]
    result = service.submit()
    assert result["patient_ids"] == []
    assert result["submission_time"] is None

    # 2. All fallback fields present, but missing nhs_number (should be included)
    df = pd.DataFrame({
        "patient_id": [2],
        "nhs_number": [None],
        "family_name": ["Brown"],
        "given_name": ["Alice"],
        "sex": ["F"],
        "date_of_birth": ["1985-05-05"],
        "postcode": ["ZZ99 1ZZ"]
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [2]
    result = service.submit()
    assert result["patient_ids"] == [2]
    assert result["submission_time"] is not None

    # 3. All nhs_trace fields present (should be included)
    df = pd.DataFrame({
        "patient_id": [3],
        "nhs_number": ["1234567890"],
        "family_name": ["White"],
        "given_name": ["Bob"],
        "sex": ["M"],
        "date_of_birth": ["1970-01-01"],
        "postcode": ["AA1 1AA"]
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [3]
    result = service.submit()
    assert result["patient_ids"] == [3]
    assert result["submission_time"] is not None

    # 4. Missing patient_id (should be excluded)
    df = pd.DataFrame({
        "patient_id": [None],
        "nhs_number": ["9999999999"],
        "family_name": ["Green"],
        "given_name": ["Charlie"],
        "sex": ["M"],
        "date_of_birth": ["1999-09-09"],
        "postcode": ["BB2 2BB"]
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [None]
    result = service.submit()
    assert result["patient_ids"] == []
    assert result["submission_time"] is None

    # 5. Missing both nhs_number and one fallback field (should be excluded)
    df = pd.DataFrame({
        "patient_id": [4],
        "nhs_number": [None],
        "family_name": ["Black"],
        "given_name": ["Dana"],
        "sex": ["F"],
        "date_of_birth": ["2001-12-12"],
        # missing postcode
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [4]
    result = service.submit()
    assert result["patient_ids"] == []
    assert result["submission_time"] is None

def test_submission_time_accuracy(service):
    df = pd.DataFrame({
        "patient_id": [1],
        "nhs_number": ["a"],
        "family_name": ["Smith"],
        "given_name": ["John"],
        "sex": ["M"],
        "date_of_birth": ["2000-01-01"],
        "postcode": ["AB1"]
    })
    service.mpi.find_unverified_patients.return_value = df
    service.trace_status.find_untraced_patients.return_value = [1]
    before = datetime.utcnow()
    result = service.submit()
    after = datetime.utcnow()
    assert result["submission_time"] is not None
    # Allow a few seconds delta
    assert before <= result["submission_time"].replace(tzinfo=None) <= after