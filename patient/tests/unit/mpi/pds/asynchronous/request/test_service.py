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
    service.mpi.find_unverified_patients.return_value = pd.DataFrame()
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
    assert result["patient_ids"] == [1]
    assert result["submission_time"] is not None

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