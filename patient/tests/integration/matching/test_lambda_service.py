import json
from pathlib import Path
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
import requests
from contextlib import contextmanager
from sqlalchemy import text

@contextmanager
def create_lambda_container_with_env(input_location_path, postgres_db, docker_network):
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

    # Set environment variables at container creation
    container.with_env("MPI_DB_USERNAME", "mpi_writer")
    container.with_env("MPI_DB_PASSWORD", "DefaultPassword123!")
    container.with_env("MPI_DB_HOST", "db")
    container.with_env("MPI_DB_NAME", "ldp")
    container.with_env("MPI_DB_PORT", "5432")
    container.with_env("MPI_SCHEMA_NAME", "mpi")

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:        
        yield running_container

def invoke_lambda(container, event, timeout: int = 30):
    """Invoke Lambda function in container"""
    port = container.get_exposed_port(8080)
      
    url = f"http://localhost:{port}/2015-03-31/functions/function/invocations"
    
    response = requests.post(url, json=event, timeout=timeout)
    return response.json()

def test_successful_single_patient_exact_match_via_database(postgres_db, docker_network):
    """
    Test that a patient request successfully queries PostgreSQL, matches an existing 
    patient record using the SQL exact match strategy, and returns the correct patient_id.
    """
    # ARRANGE: Insert a known patient into the database
    with postgres_db.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO patient (
                nhs_number, given_name, family_name, date_of_birth, 
                postcode, sex, verified, created_at, updated_at
            ) VALUES (
                '9434765919', 'John', 'Doe', '1980-01-15',
                'SW1A 1AA', 'male', true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING patient_id
        """))
        existing_patient_id = result.fetchone()[0]
        conn.commit()
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
    with create_lambda_container_with_env(None, postgres_db, docker_network) as container:
        response = invoke_lambda(container, event)
    
    # ASSERT: Verify response structure and content
    assert response["statusCode"] == 200, f"Expected 200, got {response['statusCode']}: {response}"
    
    body = response["body"]
    assert body["message"] == "Patient Matching completed successfully"
    assert body["records_processed"] == 1
    assert body["records_matched"] == 1
    
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
    
    print(f"✓ Successfully matched patient to existing record: {existing_patient_id}")