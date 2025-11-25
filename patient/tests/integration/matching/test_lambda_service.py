from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
import requests
from contextlib import contextmanager
from sqlalchemy import text
import logging

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@contextmanager
def create_lambda_container_with_env(docker_network, env_vars=None):
    """
    Helper function to create a Lambda container with environment variables.
    
    Args:
        postgres_db: PostgreSQL database engine
        docker_network: Docker network for container communication
        
    Yields:
        running_container: Ready-to-use Lambda container
    """
    container = DockerContainer("patient-matching:latest")
    container.with_exposed_ports(8080)

    # Add to the same network as PostgreSQL
    container.with_network(docker_network)

    # Default environment variables
    defaults = {
        "MPI_DB_USERNAME": "mpi_writer",
        "MPI_DB_PASSWORD": "DefaultPassword123!",
        "MPI_DB_HOST": "db",
        "MPI_DB_NAME": "ldp",
        "MPI_DB_PORT": "5432",
        "MPI_SCHEMA_NAME": "mpi"
    }

    # Use provided env_vars or defaults
    env = defaults.copy()
    if env_vars:
        env.update(env_vars)

    for key, value in env.items():
        container.with_env(key, value)    

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:        
        yield running_container

def invoke_lambda(container, event, timeout: int = 30):
    """Invoke Lambda function in container"""
    port = container.get_exposed_port(8080)
      
    url = f"http://localhost:{port}/2015-03-31/functions/function/invocations"
    
    response = requests.post(url, json=event, timeout=timeout)
    return response.json()

def make_patient(
    nhs_number=None, first_name=None, last_name=None, dob=None,
    postcode=None, sex=None, verified=None
):
    patient = {
        "nhs_number": nhs_number,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob,
        "postcode": postcode,
        "sex": sex,
    }
    if verified is not None:
        patient["verified"] = verified
    return patient

def to_event_patient(patient):
    # Remove DB-only fields
    return {k: v for k, v in patient.items() if k in {
        "nhs_number", "first_name", "last_name", "dob", "postcode", "sex"
    }}

def to_expected_patient(patient, patient_ids):
    expected = to_event_patient(patient)
    expected["patient_ids"] = patient_ids
    return expected

def insert_patients(conn, patients):
    """Insert a list of patient dicts into the database and return their IDs."""
    ids = []
    for p in patients:
        result = conn.execute(text("""
            INSERT INTO patient (
                nhs_number, given_name, family_name, date_of_birth, 
                postcode, sex, verified, created_at, updated_at
            ) VALUES (
                :nhs_number, :first_name, :last_name, :dob,
                :postcode, :sex, :verified, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING patient_id
        """), p)
        ids.append(result.fetchone()[0])
    conn.commit()
    return ids

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
        make_patient("9434765870", "Jane", "Smith", "1975-06-22", "SW1A 1AA", "female", True),
        make_patient("9434765828", "Bob", "Smith", "1990-03-10", "SW1A 1AA", "male", True)
    ]
    with postgres_db.connect() as conn:
        patient_ids = insert_patients(conn, patients)
        patient_id_1, patient_id_2, patient_id_3 = patient_ids
        print(f"Inserted 3 test patients with IDs: {patient_id_1}, {patient_id_2}, {patient_id_3}")

    # Prepare Lambda event with partial data (only last name and postcode have values)
    event_patient = make_patient(last_name="Smith", postcode="SW1A 1AA")
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
        expected_data=[to_expected_patient(event_patient, [patient_id_1, patient_id_2, patient_id_3])]
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
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
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
        make_patient("9434765870", "Jane", "Williams", "1975-06-22", "BS1 5TH", "female", False)
    ]
    with postgres_db.connect() as conn:
        verified_patient_id, unverified_patient_id = insert_patients(conn, patients)
        print(f"Inserted verified patient ID: {verified_patient_id}, unverified patient ID: {unverified_patient_id}")

    # Prepare Lambda event with partial data (only last name and postcode)
    event_patient = make_patient(last_name="Williams", postcode="BS1 5TH")
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
        make_patient("1111111111", "Alice", "Brown", "1985-03-20", "E1 6AN", "female", True),
        make_patient("2222222222", "John", "Smith", "1980-01-15", "SW1A 1AA", "male", True),
        make_patient("3333333333", "Jane", "Smith", "1975-06-22", "SW1A 1AA", "female", True),
        make_patient("4444444444", "Bob", "Smith", "1990-03-10", "SW1A 1AA", "male", True)
    ]
    with postgres_db.connect() as conn:
        ids = insert_patients(conn, patients)
        exact_match_id = ids[0]
        multi_match_id_1 = ids[1]
        multi_match_id_2 = ids[2]
        multi_match_id_3 = ids[3]
        print(f"Inserted test patients - Exact match: {exact_match_id}, Multiple match: {multi_match_id_1}, {multi_match_id_2}, {multi_match_id_3}")
    
    # Prepare Lambda event with batch of 4 patients
    event_patients = [
        make_patient("1111111111", "Alice", "Brown", "1985-03-20", "E1 6AN", "female"),
        make_patient(last_name="Smith", postcode="SW1A 1AA"),
        make_patient("9999999999", "Charlie", "Davis", "1995-07-14", "M1 1AE", "male"),
        make_patient()
    ]
    event = {
        "patients": [to_event_patient(p) for p in event_patients]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure    
    data = response["body"]["data"]
    new_patient_id = None
    for patient in data:
        if patient["nhs_number"] == "9999999999":
            new_patient_id = patient["patient_ids"][0]
    data = response["body"]["data"]
    new_patient_id = None
    for patient in data:
        if patient["nhs_number"] == "9999999999":
            new_patient_id = patient["patient_ids"][0]
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 4, "single": 2, "multiple": 1, "zero": 1},
        expected_data=[
            to_expected_patient(event_patients[0], [exact_match_id]),
            to_expected_patient(event_patients[1], [multi_match_id_1, multi_match_id_2, multi_match_id_3]),
            to_expected_patient(event_patients[2], [new_patient_id]),
            to_expected_patient(event_patients[3], [])
        ]
    )

def test_empty_and_missing_optional_fields_handling(postgres_db, docker_network):
    """
    Test that the system correctly handles various combinations of missing/empty optional fields:
    - Patient 1: Only NHS number populated (single field search)
    - Patient 2: Only name fields populated
    - Patient 3: Empty strings mixed with valid values (should treat empty as None)
    - Patient 4: Only surname populated (single field search)
    """
    # ARRANGE: Insert test patients into the database
    patients = [
        make_patient("5555555555", "Tom", "Jones", "1970-05-10", "NW1 2DB", "male", True),
        make_patient("6666666666", "Sarah", "Wilson", "1988-11-25", "B1 1AA", "female", True),
        make_patient("7777777777", "David", "Taylor", "1992-02-14", "M2 3EF", "male", True)
    ]
    with postgres_db.connect() as conn:
        nhs_match_id, name_match_id, surname_match_id = insert_patients(conn, patients)
        print(f"Inserted test patients - NHS: {nhs_match_id}, Name: {name_match_id}, Surname: {surname_match_id}")
    
    # Prepare Lambda event with various empty/missing field combinations
    event_patients = [
        make_patient("5555555555"),
        make_patient(first_name="Sarah", last_name="Wilson"),
        make_patient(first_name="Alice", postcode="SW1A 1AA"),
        make_patient(last_name="Taylor")
    ]
    event = {
        "patients": [to_event_patient(p) for p in event_patients]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure    
    data = response["body"]["data"]
    new_patient_id = None
    for patient in data:
        if patient["first_name"] == "Alice" and patient["postcode"] == "SW1A 1AA":
            new_patient_id = patient["patient_ids"][0]
    data = response["body"]["data"]
    new_patient_id = None
    for patient in data:
        if patient["first_name"] == "Alice" and patient["postcode"] == "SW1A 1AA":
            new_patient_id = patient["patient_ids"][0]
    assert_response(
        response,
        expected_status=200,
        expected_message="Patient Matching completed successfully",
        expected_counts={"total": 4, "single": 4, "multiple": 0, "zero": 0},
        expected_data=[
            to_expected_patient(event_patients[0], [nhs_match_id]),
            to_expected_patient(event_patients[1], [name_match_id]),
            to_expected_patient(event_patients[2], [new_patient_id]),
            to_expected_patient(event_patients[3], [surname_match_id])
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