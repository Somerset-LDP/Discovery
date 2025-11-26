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
        input_location_path: Path to GP records CSV file  
        postgres_db: PostgreSQL database engine
        docker_network: Docker network for container communication
        
    Yields:
        running_container: Ready-to-use Lambda container
    """
    container = DockerContainer("canonical_layer:latest")
    container.with_exposed_ports(8080)

    # Map host files to container paths
    container.with_volume_mapping(input_location_path, "/input", "ro")  # read-only

    # Add to the same network as PostgreSQL
    container.with_network(docker_network)

    # Set environment variables at container creation
    container.with_env("OUTPUT_DB_USERNAME", "canonical_writer")
    container.with_env("OUTPUT_DB_PASSWORD", "DefaultPassword123!")
    container.with_env("OUTPUT_DB_HOST", "db")
    container.with_env("OUTPUT_DB_NAME", "ldp")
    container.with_env("OUTPUT_DB_PORT", "5432")

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:        
        yield running_container

def invoke_lambda(container, event: dict, timeout: int = 30):
    """
    Invoke Lambda function in container.

    Args:
        container: Running Lambda container
        event: Event payload with feed_type and input_path
        timeout: Request timeout in seconds

    Returns:
        Lambda response as dict
    """
    port = container.get_exposed_port(8080)
      
    url = f"http://localhost:{port}/2015-03-31/functions/function/invocations"
    
    response = requests.post(url, json=event, timeout=timeout)
    return response.json()

def test_valid_input(postgres_db, docker_network):
    fixtures_path = Path(__file__).parent.parent / "fixtures"
    input_location = str(fixtures_path / "emis_patients.csv")

    expected_patients = [
        {
            "nhs_number": "1112223333",
            "given_name": "Leonard",
            "family_name": "Morse",
            "sex": "Male"
        },
        {
            "nhs_number": "2221113333",
            "given_name": "Skylar",
            "family_name": "Spork",
            "sex": "Female"
        }
    ]    

    with create_lambda_container_with_env(input_location, postgres_db, docker_network) as container:
        # check that the canonical.patients table is empty
        with postgres_db.connect() as conn:

            # patient table should be empty
            result = conn.execute(text("SELECT COUNT(*) FROM canonical.patient")).scalar_one()
            assert result == 0, f"Expected 0 records in canonical.patient, got {result}"

            # Invoke Lambda with proper event structure
            event = {
                "feed_type": "gp",
                "input_path": "file:///input"
            }
            result = invoke_lambda(container, event)
            
            assert result["statusCode"] == 200
            
            response_body = json.loads(result["body"])
            assert "records_processed" in response_body
            assert "records_stored" in response_body
            assert "feed_type" in response_body
            assert response_body["records_processed"] == 2, f"Expected 2 records processed, got {response_body['records_processed']}"
            assert response_body["records_stored"] == 2, f"Expected 2 records stored, got {response_body['records_stored']}"
            assert response_body["feed_type"] == "gp", f"Expected feed_type 'gp', got {response_body['feed_type']}"

            # verify db contents
            result = conn.execute(text("SELECT COUNT(*) FROM canonical.patient")).scalar_one()
            assert result == 2, f"Expected 2 patients after insertion, found {result}"

            actual_patients = conn.execute(text("SELECT * FROM canonical.patient")).mappings().fetchall()
            for i, (actual, expected) in enumerate(zip(actual_patients, expected_patients)):
                for field, expected_value in expected.items():
                    actual_value = actual[field]
                    assert actual_value == expected_value, \
                        f"Patient {i+1} - Expected {field} '{expected_value}', got '{actual_value}'"            
