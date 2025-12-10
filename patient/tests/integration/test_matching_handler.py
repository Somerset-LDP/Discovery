from sqlalchemy import text
from fixtures.aws import create_lambda_container_with_env, invoke_lambda
from fixtures.patients import make_patient, insert_patients
import logging

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def to_event_patient(patient):
    # Remove DB-only fields
    return {k: v for k, v in patient.items() if k in {
        "nhs_number", "first_name", "last_name", "dob", "postcode", "sex"
    }}

def to_expected_patient(patient, patient_ids):
    expected = to_event_patient(patient)
    expected["patient_ids"] = patient_ids
    return expected

def assert_response(response, expected_status=200, expected_message=None, expected_counts=None, expected_data=None):
    """
    Assert the Lambda response matches expected status, message, counts, and optionally patient data.
    expected_counts: dict or None, expected values for keys in response['body']['counts']
    expected_data: list or None, expected patient data dicts in response['body']['data']
    """
    assert response["statusCode"] == expected_status, f"Expected status {expected_status}, got {response['statusCode']}"
    
    body = response["body"]
    if expected_message is not None:
        assert body["message"] == expected_message, f"Expected message '{expected_message}', got '{body['message']}'"
    
    if expected_counts is not None:
        counts = body["counts"]
        for k, v in expected_counts.items():
            assert counts[k] == v, f"Expected counts['{k}']={v}, got {counts[k]}"
    
    if expected_data is not None:
        data = body["data"]
        assert len(data) == len(expected_data), f"Expected {len(expected_data)} data items, got {len(data)}"
        for expected, actual in zip(expected_data, data):
            print(f"Checking expected patient data: {expected} against actual: {actual}")
            for k, v in expected.items():
                assert actual.get(k) == v, f"Expected data field {k}={v}, got {actual.get(k)}"

def test_successful_single_patient_exact_match_via_database(postgres_db, docker_network):
    """
    Test that a patient request successfully queries PostgreSQL, matches an existing 
    patient record using the SQL exact match strategy, and returns the correct patient_id.
    """
    # ARRANGE: Create and insert a known patient
    patient = make_patient(
        nhs_number="9434765919",
        first_name="John",
        last_name="Doe",
        dob="1980-01-15",
        postcode="SW1A 1AA",
        sex="male",
        verified=True
    )
    with postgres_db.connect() as conn:
        [existing_patient_id] = insert_patients(conn, [patient])
        print(f"Inserted test patient with ID: {existing_patient_id}")

    # Prepare Lambda event
    event = {
        "patients": [to_event_patient(patient)]
    }

    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)

    # ASSERT: Use helper for response assertions
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 1, "single": 1, "multiple": 0, "zero": 0},
        expected_data=[to_expected_patient(patient, [existing_patient_id])]
    )

def test_multiple_patient_match_returns_all_ids_from_database(postgres_db, docker_network):
    """
    Test that a partial query (e.g., only surname and postcode) executes a SQL query 
    that returns multiple matching patient_ids from actual database records.
    """
    # ARRANGE: Insert multiple patients with same surname and postcode
    patients = [
        make_patient("9434765919", "John", "Smith", "1980-01-15", "SW1A 1AA", "male", True),
        make_patient("9434765870", "John", "Smith", "1980-01-15", "SW1A 1AA", "male", True),
        make_patient("9434765828", "Bob", "Smith", "1990-03-10", "SW1A 1AA", "male", True)
    ]
    with postgres_db.connect() as conn:
        patient_ids = insert_patients(conn, patients)
        patient_id_1, patient_id_2, patient_id_3 = patient_ids
        print(f"Inserted 3 test patients with IDs: {patient_id_1}, {patient_id_2}, {patient_id_3}")

    # Prepare Lambda event with query that matches multiple patients
    event_patient = make_patient(first_name="John", last_name="Smith", dob="1980-01-15", postcode="SW1A 1AA", sex="male")
    event = {
        "patients": [to_event_patient(event_patient)]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Use helper for response assertions
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 1, "single": 0, "multiple": 1, "zero": 0},
        expected_data=[to_expected_patient(event_patient, [patient_id_1, patient_id_2])]
    )

def test_no_match_creates_new_unverified_patient(postgres_db, docker_network):
    """
    Test that when a patient query finds no matches in the database, 
    a new unverified patient record is created and its patient_id is returned.
    """
    # ARRANGE: Verify database is initially empty
    with postgres_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar_one()
        assert result == 0, f"Expected empty patient table, got {result} records"
    
    # Prepare Lambda event with a patient that doesn't exist
    patient = make_patient(
        nhs_number="9876543210",
        first_name="Alice",
        last_name="Johnson",
        postcode="M1 2AB",
        dob="1992-04-18",
        sex="female"
    )
    event = {
        "patients": [to_event_patient(patient)]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network=docker_network, env_vars={"LOG_LEVEL": "DEBUG"}) as container:
        response = invoke_lambda(container, event)
        #print(container.get_logs())
    
    # ASSERT: Use helper for response assertions
    new_patient = response["body"]["data"][0]
    new_patient_id = new_patient["patient_ids"][0]
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 1, "single": 1, "multiple": 0, "zero": 0},
        expected_data=[to_expected_patient(patient, [new_patient_id])]
    )
    
    # VERIFY: Check the database to confirm the patient was inserted as unverified
    with postgres_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar_one()
        print(f"Total patients in database after insertion: {result}")

        result = conn.execute(text("""
            SELECT patient_id, nhs_number, given_name, family_name, 
                    date_of_birth, postcode, sex, verified
            FROM patient
            WHERE patient_id = :patient_id
        """), {"patient_id": new_patient_id}).mappings().fetchone()
        
        assert result is not None, f"Patient {new_patient_id} not found in database"
        assert result["patient_id"] == new_patient_id
        assert result["nhs_number"] == "9876543210"
        assert result["given_name"] == "Alice"
        assert result["family_name"] == "Johnson"
        assert result["date_of_birth"] == "1992-04-18"
        assert result["postcode"] == "M1 2AB"
        assert result["sex"] == "female"
        assert result["verified"] is False, "New patient should be created as unverified"
        
        # Verify only one record was created
        total_count = conn.execute(text("SELECT COUNT(*) FROM patient")).scalar_one()
        assert total_count == 1, f"Expected 1 patient record, found {total_count}"    

def test_verified_patient_match_excludes_unverified(postgres_db, docker_network):
    """
    Test that the SQL matching strategy only returns verified patient records.
    When multiple patients match the search criteria but one is unverified,
    only the verified patient's ID should be returned.
    """
    # ARRANGE: Insert two patients with same surname and postcode - one verified, one unverified
    patients = [
        make_patient("9434765919", "John", "Williams", "1980-01-15", "BS1 5TH", "male", True),
        make_patient("9434765870", "John", "Williams", "1980-01-15", "BS1 5TH", "male", False),
    ]
    with postgres_db.connect() as conn:
        verified_patient_id, unverified_patient_id = insert_patients(conn, patients)
        print(f"Inserted verified patient ID: {verified_patient_id}, unverified patient ID: {unverified_patient_id}")

    # Prepare Lambda event with partial data (only last name and postcode)
    event_patient = make_patient(first_name="John", last_name="Williams", dob="1980-01-15", postcode="BS1 5TH", sex="male")
    event = {
        "patients": [to_event_patient(event_patient)]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Use helper for response assertions
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 1, "single": 1, "multiple": 0, "zero": 0},
        expected_data=[to_expected_patient(event_patient, [verified_patient_id])]
    )

def test_batch_request_with_mixed_matching_outcomes(postgres_db, docker_network):
    """
    Test that a batch of multiple patients is processed independently with mixed outcomes:
    - Patient 1: Exact match to existing verified patient
    - Patient 2: Multiple matches (ambiguous)
    - Patient 3: No match, creates new unverified patient
    - Patient 4: Insufficient data, rejected (zero patient_ids)
    """
    # ARRANGE: Insert test patients into the database

    patients = [
        make_patient("9434765919", "Alice", "Brown", "1985-03-20", "E1 6AN", "female", True),
        make_patient("4857773456", "John", "Smith", "1980-01-15", "SW1A 1AA", "male", True),
        make_patient("9876543210", "John", "Smith", "1980-01-15", "SW1A 1AA", "male", True),
        make_patient("4857773457", "Jane", "Smith", "1975-06-22", "SW1A 1AA", "female", True),
        make_patient("4857773465", "Bob", "Smith", "1990-03-10", "SW1A 1AA", "male", True)
    ]
    with postgres_db.connect() as conn:
        patient_ids = insert_patients(conn, patients)
    
    # Prepare Lambda event with batch of 4 patients
    event_patients = [
        make_patient(nhs_number="9434765919", dob="1985-03-20"),  # Exact match
        make_patient(first_name="John", last_name="Smith", dob="1980-01-15", postcode="SW1A 1AA", sex="male"),  # Multiple matches
        make_patient(first_name="Charlie", last_name="Davis", dob="1995-07-14", postcode="M1 1AE", sex="male"), # No match
        make_patient(first_name="Charlie", last_name=None, dob="1995-07-14", postcode="M1 1AE", sex="male"), # # Insufficient data - last name None
        make_patient() # Insufficient data - no fields
    ]
    event = {
        "patients": [to_event_patient(p) for p in event_patients]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
 
    # Extract new patient ID from response for Patient 3
    data = response["body"]["data"]
    new_patient_id = None
    for patient in data:
        if patient["first_name"] == "Charlie" and patient["last_name"] == "Davis":
            new_patient_id = patient["patient_ids"][0]        

    # ASSERT: Verify response structure    
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 5, "single": 2, "multiple": 1, "zero": 2},
        expected_data=[
            to_expected_patient(event_patients[0], [patient_ids[0]]),
            to_expected_patient(event_patients[1], [patient_ids[1], patient_ids[2]]),
            to_expected_patient(event_patients[2], [new_patient_id]),
            to_expected_patient(event_patients[3], []),
            to_expected_patient(event_patients[4], [])
        ]
    )

def test_database_connection_failure_returns_error(postgres_db, docker_network):
    """
    Test that the Lambda returns a clear error response when it cannot connect to the database.
    Simulate by passing an invalid DB host.
    """
    # Prepare Lambda event with a valid patient
    event = {
        "patients": [
            {
                "nhs_number": "1234567890",
                "first_name": "John",
                "last_name": "Doe",
                "postcode": "SW1A 1AA",
                "dob": "1980-01-15",
                "sex": "male"
            }
        ]
    }

    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network, env_vars={"MPI_DB_HOST": "invalid_host"}) as container:
        response = invoke_lambda(container, event)

    # ASSERT: Should return 500 error with clear message
    assert response["statusCode"] == 500, f"Expected 500, got {response['statusCode']}: {response}"
    body = response["body"]
    assert "Failed to create database engine" in body["message"]