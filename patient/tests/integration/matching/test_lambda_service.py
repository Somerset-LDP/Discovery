import json
from pathlib import Path
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

def test_successful_single_patient_exact_match_via_database(postgres_db, docker_network):
    """
    Test that a patient request successfully queries PostgreSQL, matches an existing 
    patient record using the SQL exact match strategy, and returns the correct patient_id.
    """
    # ARRANGE: Insert a known patient into the database
    patient_data = [{
        "nhs_number": "9434765919",
        "first_name": "John",
        "last_name": "Doe",
        "dob": "1980-01-15",
        "postcode": "SW1A 1AA",
        "sex": "male",
        "verified": True
    }]
    with postgres_db.connect() as conn:
        [existing_patient_id] = insert_patients(conn, patient_data)
        print(f"Inserted test patient with ID: {existing_patient_id}")
    
    # Prepare Lambda event
    event = {
        "patients": [
            {
                "nhs_number": "9434765919",
                "first_name": "John",
                "last_name": "Doe",
                "postcode": "SW1A 1AA",
                "dob": "1980-01-15",
                "sex": "male"
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure and content
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"

    # Verify counts
    counts = body["counts"]
    assert counts["total"] == 1
    assert counts["single"] == 1
    assert counts["multiple"] == 0
    assert counts["zero"] == 0
    
    # Verify the matched patient data
    data = body["data"]
    assert len(data) == 1
    
    matched_patient = data[0]
    assert matched_patient["nhs_number"] == "9434765919"
    assert matched_patient["first_name"] == "John"
    assert matched_patient["last_name"] == "Doe"
    assert matched_patient["postcode"] == "SW1A 1AA"
    assert matched_patient["dob"] == "1980-01-15"
    assert matched_patient["sex"] == "male"
    
    # Most importantly: verify the patient_id matches the existing record
    assert "patient_ids" in matched_patient
    assert isinstance(matched_patient["patient_ids"], list)
    assert len(matched_patient["patient_ids"]) == 1
    assert matched_patient["patient_ids"][0] == existing_patient_id

def test_multiple_patient_match_returns_all_ids_from_database(postgres_db, docker_network):
    """
    Test that a partial query (e.g., only surname and postcode) executes a SQL query 
    that returns multiple matching patient_ids from actual database records.
    """
    # ARRANGE: Insert multiple patients with same surname and postcode
    patient_data = [
        {
            "nhs_number": "9434765919",
            "first_name": "John",
            "last_name": "Smith",
            "dob": "1980-01-15",
            "postcode": "SW1A 1AA",
            "sex": "male",
            "verified": True
        },
        {
            "nhs_number": "9434765870",
            "first_name": "Jane",
            "last_name": "Smith",
            "dob": "1975-06-22",
            "postcode": "SW1A 1AA",
            "sex": "female",
            "verified": True
        },
        {
            "nhs_number": "9434765828",
            "first_name": "Bob",
            "last_name": "Smith",
            "dob": "1990-03-10",
            "postcode": "SW1A 1AA",
            "sex": "male",
            "verified": True
        }
    ]
    with postgres_db.connect() as conn:
        patient_ids = insert_patients(conn, patient_data)
        patient_id_1, patient_id_2, patient_id_3 = patient_ids
        print(f"Inserted 3 test patients with IDs: {patient_id_1}, {patient_id_2}, {patient_id_3}")
    
    # Prepare Lambda event with partial data (only last name and postcode have values)
    event = {
        "patients": [
            {
                "nhs_number": None,
                "first_name": None,
                "last_name": "Smith",
                "dob": None,
                "postcode": "SW1A 1AA",
                "sex": None
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"

    # Verify counts
    counts = body["counts"]
    assert counts["total"] == 1
    assert counts["single"] == 0
    assert counts["multiple"] == 1
    assert counts["zero"] == 0
    
    # Verify the matched patient data contains all three patient IDs
    data = body["data"]
    assert len(data) == 1
    
    matched_patient = data[0]
    assert matched_patient["last_name"] == "Smith"
    assert matched_patient["postcode"] == "SW1A 1AA"
    
    # Most importantly: verify all three patient_ids are returned
    assert "patient_ids" in matched_patient
    assert isinstance(matched_patient["patient_ids"], list)
    assert len(matched_patient["patient_ids"]) == 3, f"Expected 3 matches, got {len(matched_patient['patient_ids'])}"
    
    # Verify all three patient IDs are in the result (order may vary)
    returned_ids = set(matched_patient["patient_ids"])
    expected_ids = {patient_id_1, patient_id_2, patient_id_3}
    assert returned_ids == expected_ids, f"Expected {expected_ids}, got {returned_ids}"

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
    event = {
        "patients": [
            {
                "nhs_number": "9876543210",
                "first_name": "Alice",
                "last_name": "Johnson",
                "postcode": "M1 2AB",
                "dob": "1992-04-18",
                "sex": "female"
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"

    # Verify counts - single match because a new unverified patient was created
    counts = body["counts"]
    assert counts["total"] == 1
    assert counts["single"] == 1, "New unverified patient should be created with single patient_id"
    assert counts["multiple"] == 0
    assert counts["zero"] == 0
    
    # Verify the response contains the newly created patient
    data = body["data"]
    assert len(data) == 1
    
    new_patient = data[0]
    assert new_patient["nhs_number"] == "9876543210"
    assert new_patient["first_name"] == "Alice"
    assert new_patient["last_name"] == "Johnson"
    assert new_patient["postcode"] == "M1 2AB"
    assert new_patient["dob"] == "1992-04-18"
    assert new_patient["sex"] == "female"
    
    # Most importantly: verify a new patient_id was created
    assert "patient_ids" in new_patient
    assert isinstance(new_patient["patient_ids"], list)
    assert len(new_patient["patient_ids"]) == 1
    new_patient_id = new_patient["patient_ids"][0]
    assert isinstance(new_patient_id, int)
    
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
    patient_data = [
        {
            "nhs_number": "9434765919",
            "first_name": "John",
            "last_name": "Williams",
            "dob": "1980-01-15",
            "postcode": "BS1 5TH",
            "sex": "male",
            "verified": True
        },
        {
            "nhs_number": "9434765870",
            "first_name": "Jane",
            "last_name": "Williams",
            "dob": "1975-06-22",
            "postcode": "BS1 5TH",
            "sex": "female",
            "verified": False
        }
    ]
    with postgres_db.connect() as conn:
        verified_patient_id, unverified_patient_id = insert_patients(conn, patient_data)
        print(f"Inserted verified patient ID: {verified_patient_id}, unverified patient ID: {unverified_patient_id}")
    
    # Prepare Lambda event with partial data (only last name and postcode)
    event = {
        "patients": [
            {
                "nhs_number": None,
                "first_name": None,
                "last_name": "Williams",
                "dob": None,
                "postcode": "BS1 5TH",
                "sex": None
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"
    
    # Verify counts - should have single match (only the verified patient)
    counts = body["counts"]
    assert counts["total"] == 1
    assert counts["single"] == 1, "Should only match the verified patient"
    assert counts["multiple"] == 0
    assert counts["zero"] == 0
    
    # Verify the matched patient data
    data = body["data"]
    assert len(data) == 1
    
    matched_patient = data[0]
    assert matched_patient["last_name"] == "Williams"
    assert matched_patient["postcode"] == "BS1 5TH"
    
    # Most importantly: verify only the verified patient_id is returned
    assert "patient_ids" in matched_patient
    assert isinstance(matched_patient["patient_ids"], list)
    assert len(matched_patient["patient_ids"]) == 1, "Should only return the verified patient"
    assert matched_patient["patient_ids"][0] == verified_patient_id, "Should return the verified patient ID"
    assert unverified_patient_id not in matched_patient["patient_ids"], "Should NOT return the unverified patient ID"

def test_batch_request_with_mixed_matching_outcomes(postgres_db, docker_network):
    """
    Test that a batch of multiple patients is processed independently with mixed outcomes:
    - Patient 1: Exact match to existing verified patient
    - Patient 2: Multiple matches (ambiguous)
    - Patient 3: No match, creates new unverified patient
    - Patient 4: Insufficient data, rejected (zero patient_ids)
    """
    # ARRANGE: Insert test patients into the database
    patient_data = [
        {
            "nhs_number": "1111111111",
            "first_name": "Alice",
            "last_name": "Brown",
            "dob": "1985-03-20",
            "postcode": "E1 6AN",
            "sex": "female",
            "verified": True
        },
        {
            "nhs_number": "2222222222",
            "first_name": "John",
            "last_name": "Smith",
            "dob": "1980-01-15",
            "postcode": "SW1A 1AA",
            "sex": "male",
            "verified": True
        },
        {
            "nhs_number": "3333333333",
            "first_name": "Jane",
            "last_name": "Smith",
            "dob": "1975-06-22",
            "postcode": "SW1A 1AA",
            "sex": "female",
            "verified": True
        },
        {
            "nhs_number": "4444444444",
            "first_name": "Bob",
            "last_name": "Smith",
            "dob": "1990-03-10",
            "postcode": "SW1A 1AA",
            "sex": "male",
            "verified": True
        }
    ]
    with postgres_db.connect() as conn:
        ids = insert_patients(conn, patient_data)
        exact_match_id = ids[0]
        multi_match_id_1 = ids[1]
        multi_match_id_2 = ids[2]
        multi_match_id_3 = ids[3]
        print(f"Inserted test patients - Exact match: {exact_match_id}, Multiple match: {multi_match_id_1}, {multi_match_id_2}, {multi_match_id_3}")
    
    # Prepare Lambda event with batch of 4 patients
    event = {
        "patients": [
            # Patient 1: Exact match
            {
                "nhs_number": "1111111111",
                "first_name": "Alice",
                "last_name": "Brown",
                "postcode": "E1 6AN",
                "dob": "1985-03-20",
                "sex": "female"
            },
            # Patient 2: Multiple matches (partial data - surname and postcode only)
            {
                "nhs_number": None,
                "first_name": None,
                "last_name": "Smith",
                "dob": None,
                "postcode": "SW1A 1AA",
                "sex": None
            },
            # Patient 3: No match, will create new unverified patient
            {
                "nhs_number": "9999999999",
                "first_name": "Charlie",
                "last_name": "Davis",
                "postcode": "M1 1AE",
                "dob": "1995-07-14",
                "sex": "male"
            },
            # Patient 4: Insufficient data (all None)
            {
                "nhs_number": None,
                "first_name": None,
                "last_name": None,
                "dob": None,
                "postcode": None,
                "sex": None
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"
    
    # Verify counts across all patients
    counts = body["counts"]
    assert counts["total"] == 4, "Should process all 4 patients"
    assert counts["single"] == 2, "Patient 1 (exact match) and Patient 3 (new unverified) should have single patient_id"
    assert counts["multiple"] == 1, "Patient 2 should have multiple matches"
    assert counts["zero"] == 1, "Patient 4 should be rejected (insufficient data)"
    
    # Verify the data for each patient
    data = body["data"]
    assert len(data) == 4
    
    # Patient 1: Exact match
    patient1 = data[0]
    assert patient1["nhs_number"] == "1111111111"
    assert patient1["first_name"] == "Alice"
    assert patient1["last_name"] == "Brown"
    assert len(patient1["patient_ids"]) == 1
    assert patient1["patient_ids"][0] == exact_match_id
    
    # Patient 2: Multiple matches
    patient2 = data[1]
    assert patient2["last_name"] == "Smith"
    assert patient2["postcode"] == "SW1A 1AA"
    assert len(patient2["patient_ids"]) == 3, f"Expected 3 matches, got {len(patient2['patient_ids'])}"
    returned_ids = set(patient2["patient_ids"])
    expected_ids = {multi_match_id_1, multi_match_id_2, multi_match_id_3}
    assert returned_ids == expected_ids, f"Expected {expected_ids}, got {returned_ids}"
    
    # Patient 3: New unverified patient created
    patient3 = data[2]
    assert patient3["nhs_number"] == "9999999999"
    assert patient3["first_name"] == "Charlie"
    assert patient3["last_name"] == "Davis"
    assert len(patient3["patient_ids"]) == 1
    new_patient_id = patient3["patient_ids"][0]
    assert isinstance(new_patient_id, int)
    
    # Verify patient 3 was inserted as unverified
    with postgres_db.connect() as conn:
        result = conn.execute(text("""
            SELECT verified FROM patient WHERE patient_id = :patient_id
        """), {"patient_id": new_patient_id}).scalar_one()
        assert result is False, "New patient should be unverified"
    
    # Patient 4: Rejected (insufficient data)
    patient4 = data[3]
    assert len(patient4["patient_ids"]) == 0, "Patient with no searchable data should have empty patient_ids"

def test_empty_and_missing_optional_fields_handling(postgres_db, docker_network):
    """
    Test that the system correctly handles various combinations of missing/empty optional fields:
    - Patient 1: Only NHS number populated (single field search)
    - Patient 2: Only name fields populated
    - Patient 3: Empty strings mixed with valid values (should treat empty as None)
    - Patient 4: Only surname populated (single field search)
    """
    # ARRANGE: Insert test patients into the database
    patient_data = [
        {
            "nhs_number": "5555555555",
            "first_name": "Tom",
            "last_name": "Jones",
            "dob": "1970-05-10",
            "postcode": "NW1 2DB",
            "sex": "male",
            "verified": True
        },
        {
            "nhs_number": "6666666666",
            "first_name": "Sarah",
            "last_name": "Wilson",
            "dob": "1988-11-25",
            "postcode": "B1 1AA",
            "sex": "female",
            "verified": True
        },
        {
            "nhs_number": "7777777777",
            "first_name": "David",
            "last_name": "Taylor",
            "dob": "1992-02-14",
            "postcode": "M2 3EF",
            "sex": "male",
            "verified": True
        }
    ]
    with postgres_db.connect() as conn:
        nhs_match_id, name_match_id, surname_match_id = insert_patients(conn, patient_data)
        print(f"Inserted test patients - NHS: {nhs_match_id}, Name: {name_match_id}, Surname: {surname_match_id}")
    
    # Prepare Lambda event with various empty/missing field combinations
    event = {
        "patients": [
            # Patient 1: Only NHS number (all other fields None)
            {
                "nhs_number": "5555555555",
                "first_name": None,
                "last_name": None,
                "dob": None,
                "postcode": None,
                "sex": None
            },
            # Patient 2: Only name fields populated
            {
                "nhs_number": None,
                "first_name": "Sarah",
                "last_name": "Wilson",
                "dob": None,
                "postcode": None,
                "sex": None
            },
            # Patient 3: Empty strings mixed with valid values
            # Empty strings should be treated as None/missing
            {
                "nhs_number": "",
                "first_name": "Alice",
                "last_name": "",
                "dob": None,
                "postcode": "SW1A 1AA",
                "sex": ""
            },
            # Patient 4: Only surname populated (single field)
            {
                "nhs_number": None,
                "first_name": None,
                "last_name": "Taylor",
                "dob": None,
                "postcode": None,
                "sex": None
            }
        ]
    }
    
    # ACT: Invoke Lambda
    with create_lambda_container_with_env(docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"
    
    # Verify counts
    counts = body["counts"]
    assert counts["total"] == 4, "Should process all 4 patients"
    # Patients 1, 2, and 4 should match existing records (single matches)
    # Patient 3 has searchable data (first_name and postcode) but won't match, so creates new unverified
    assert counts["single"] == 4, "Should have 4 single matches/creations"
    assert counts["multiple"] == 0
    assert counts["zero"] == 0
    
    # Verify the data for each patient
    data = body["data"]
    assert len(data) == 4
    
    # Patient 1: NHS number only match
    patient1 = data[0]
    assert patient1["nhs_number"] == "5555555555"
    assert len(patient1["patient_ids"]) == 1
    assert patient1["patient_ids"][0] == nhs_match_id, "Should match by NHS number alone"
    
    # Patient 2: Name only match
    patient2 = data[1]
    assert patient2["first_name"] == "Sarah"
    assert patient2["last_name"] == "Wilson"
    assert len(patient2["patient_ids"]) == 1
    assert patient2["patient_ids"][0] == name_match_id, "Should match by name alone"
    
    # Patient 3: Empty strings treated as None, creates new unverified patient
    # Has first_name and postcode populated, but won't match existing records
    patient3 = data[2]
    assert patient3["first_name"] == "Alice"
    assert patient3["postcode"] == "SW1A 1AA"
    # Empty strings should be cleaned to None
    assert patient3["nhs_number"] is None or patient3["nhs_number"] == ""
    assert patient3["last_name"] is None or patient3["last_name"] == ""
    assert patient3["sex"] is None or patient3["sex"] == ""
    assert len(patient3["patient_ids"]) == 1
    new_patient_id = patient3["patient_ids"][0]
    
    # Verify patient 3 was created as unverified
    with postgres_db.connect() as conn:
        result = conn.execute(text("""
            SELECT verified, given_name, postcode FROM patient WHERE patient_id = :patient_id
        """), {"patient_id": new_patient_id}).mappings().fetchone()
        assert result is not None
        assert result["verified"] is False, "New patient should be unverified"
        assert result["given_name"] == "Alice"
        assert result["postcode"] == "SW1A 1AA"
    
    # Patient 4: Surname only match
    patient4 = data[3]
    assert patient4["last_name"] == "Taylor"
    assert len(patient4["patient_ids"]) == 1
    assert patient4["patient_ids"][0] == surname_match_id, "Should match by surname alone"

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