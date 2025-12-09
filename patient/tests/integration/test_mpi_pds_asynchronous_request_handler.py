from sqlalchemy import text
from fixtures.aws import create_lambda_container_with_env, invoke_lambda
from fixtures.patients import make_patient, insert_patients
from datetime import datetime
from typing import Optional, List
import logging

from mpi.pds.asynchronous.request.service import SubmitStatus

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def assert_response(
    response: dict,
    expected_status: int = 200,
    expected_message: Optional[str] = None,
    expected_request_id: Optional[str] = None,
    expected_status_obj: Optional[SubmitStatus] = None
):
    assert isinstance(response, dict), "Response should be a dict"
    assert "statusCode" in response, "Response missing statusCode"
    assert response["statusCode"] == expected_status, f"Expected statusCode {expected_status}, got {response['statusCode']}"

    assert "body" in response, "Response missing body"
    body = response["body"]
    assert isinstance(body, dict), "Response body should be a dict"

    if expected_message is not None:
        assert "message" in body, "Response body missing 'message'"
        assert expected_message in body["message"], f"Expected message '{expected_message}', got '{body['message']}'"

    if expected_request_id is not None:
        assert "request_id" in body, "Response body missing 'request_id'"
        assert body["request_id"] == expected_request_id, f"Expected request_id '{expected_request_id}', got '{body['request_id']}'"

    if expected_status_obj is not None:
        assert "status" in body, "Response body missing 'status'"
        
        status = body["status"]
        assert isinstance(status, dict), "Status should be a dict"

        assert "patient_ids" in status, "Status should contain 'patient_ids'"
        assert isinstance(status["patient_ids"], list), "'patient_ids' should be a list"

        assert "submission_time" in status, "Status should contain 'submission_time'"
        
        # Convert submission_time to datetime for comparison if needed
        expected_time = expected_status_obj["submission_time"]
        actual_time = status["submission_time"]
        if isinstance(actual_time, str):
            actual_time = datetime.fromisoformat(actual_time)
        
        # Compare both fields exactly
        assert status["patient_ids"] == expected_status_obj["patient_ids"], f"Expected patient_ids {expected_status_obj['patient_ids']}, got {status['patient_ids']}"
        assert actual_time == expected_time, f"Expected submission_time {expected_time}, got {actual_time}"

def test_successful_submission_with_unverified_untraced_patients(postgres_db, docker_network):
    """
    Pre-populate DB with valid, untraced patients (one unverified, one verified).
    Invoke lambda_handler and assert only the unverified patient is processed.
    """
    patient_unverified = make_patient(
        nhs_number="1234567890",
        first_name="Jane",
        last_name="Doe",
        dob="1975-05-20",
        postcode="AB12 3CD",
        sex="female",
        verified=False
    )
    patient_verified = make_patient(
        nhs_number="9876543210",
        first_name="John",
        last_name="Smith",
        dob="1980-01-01",
        postcode="XY98 7ZT",
        sex="male",
        verified=True
    )
    with postgres_db.connect() as conn:
        unverified_id, verified_id = insert_patients(conn,  [patient_unverified, patient_verified])

        # Ensure neither patient is marked as submitted before Lambda invocation
        result = conn.execute(text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"), {"pid": unverified_id}).scalar_one_or_none()
        assert result is None, "Unverified patient should not be marked as submitted before Lambda invocation"

        result = conn.execute(text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"), {"pid": verified_id}).scalar_one_or_none()
        assert result is None, "Verified patient should not be marked as submitted before Lambda invocation"
      
        with create_lambda_container_with_env(docker_network=docker_network, env_vars={"LOG_LEVEL": "DEBUG"}, image="patient-mpi-pds-asynchronous-request:latest") as container:
            response = invoke_lambda(container, {})
            #print(container.get_logs())

        # Check trace_status table 
        submission_time = conn.execute(text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"), {"pid": verified_id}).scalar_one_or_none()
        assert submission_time is None, "Verified patient should NOT be marked as submitted"

        submission_time = conn.execute(text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"), {"pid": unverified_id}).scalar_one_or_none()
        assert submission_time is not None, "Unverified patient should be marked as submitted"

        assert_response(
            response,
            expected_status=200,
            expected_message="PDS Trace submission completed successfully",
            expected_status_obj=SubmitStatus(patient_ids=[unverified_id], submission_time=submission_time)
        )

def test_unverified_patient_already_traced_is_skipped(postgres_db, docker_network):
    """
    If an unverified patient already has a trace_status entry, the Lambda should not process it again.
    """
    patient_unverified = make_patient(
        nhs_number="1234567890",
        first_name="Jane",
        last_name="Doe",
        dob="1975-05-20",
        postcode="AB12 3CD",
        sex="female",
        verified=False
    )
    with postgres_db.connect() as conn:
        # Insert the unverified patient
        (unverified_id,) = insert_patients(conn, [patient_unverified])

        # Insert a trace_status entry for this patient (simulate already traced)
        now = datetime.now()
        conn.execute(
            text("INSERT INTO trace_status (patient_id, submitted_at) VALUES (:pid, :submitted_at)"),
            {"pid": unverified_id, "submitted_at": now}
        )
        conn.commit()

        submitted_at = conn.execute(
            text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"),
            {"pid": unverified_id}
        ).scalar_one_or_none()
        assert submitted_at is not None, "Patient should already be marked as submitted before Lambda invocation"        

        with create_lambda_container_with_env(docker_network=docker_network, env_vars={"LOG_LEVEL": "DEBUG"}, image="patient-mpi-pds-asynchronous-request:latest") as container:
            response = invoke_lambda(container, {})
            # print(container.get_logs())

        # Ensure no new submission occurred (trace_status unchanged)
        result = conn.execute(text("SELECT submitted_at FROM trace_status WHERE patient_id = :pid"), {"pid": unverified_id}).scalar_one_or_none()
        assert result == submitted_at, "trace_status should not be updated for already traced patient"

        # The response should indicate no untraced patients were found
        assert_response(
            response,
            expected_status=200,
            expected_message="PDS Trace submission completed successfully",
            expected_status_obj=SubmitStatus(patient_ids=[], submission_time=None)
        )

def test_database_connection_failure_returns_error(postgres_db, docker_network):
    """
    Test that the Lambda returns a clear error response when it cannot connect to the database.
    Simulate by passing an invalid DB host.
    """
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network, env_vars={"MPI_DB_HOST": "invalid_host"}, image="patient-mpi-pds-asynchronous-request:latest") as container:
        response = invoke_lambda(container, {})

    # ASSERT: Should return 500 error with clear message
    assert_response(
        response,
        expected_status=500,
        expected_message="Failed to create database engine"
    )
