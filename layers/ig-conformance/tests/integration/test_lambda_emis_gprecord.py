import json
import os
from urllib import response
import pytest
from pathlib import Path
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
import requests
import tempfile
import time
from contextlib import contextmanager
        

@contextmanager
def create_lambda_container_with_env(cohort_store_path, input_location_path, output_location_path):
    """
    Helper function to create a Lambda container with environment variables.
    
    Args:
        cohort_store_path: Path to cohort CSV file
        input_location_path: Path to GP records CSV file  
        output_location_path: Path for output CSV file
        
    Yields:
        running_container: Ready-to-use Lambda container
    """
    container = DockerContainer("emis_gprecord:latest")
    container.with_exposed_ports(8080)

    # Map host files to container paths
    container.with_volume_mapping(str(cohort_store_path), "/cohort_store.csv", "ro")  # read-only
    container.with_volume_mapping(str(input_location_path), "/input.csv", "ro")  # read-only
    container.with_volume_mapping(output_location_path, "/output", "rw")  # read-write

    # Set environment variables at container creation
    container.with_env("COHORT_STORE", "/cohort_store.csv")
    container.with_env("INPUT_LOCATION", "/input.csv")
    container.with_env("OUTPUT_LOCATION", "/output/result.csv")

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:
        yield running_container

@pytest.fixture
def test_files():

    fixtures_path = Path(__file__).parent.parent / "fixtures"

    test_files = {
        "valid_cohort": str(fixtures_path / "cohort_data" / "valid_cohort.csv"),
        "empty_cohort": str(fixtures_path / "cohort_data" / "empty_file.csv"),
        "missing_nhs_cohort": str(fixtures_path / "cohort_data" / "missing_nhs_column.csv"),
        "valid_gp_records": str(fixtures_path / "gp_data" / "valid_gp_records.csv"),
        "empty_gp_records": str(fixtures_path / "gp_data" / "empty_gp_records.csv"),
    }
    
    yield test_files

def invoke_lambda(container, timeout: int = 30):
    """Invoke Lambda function in container"""
    port = container.get_exposed_port(8080)
      
    url = f"http://localhost:{port}/2015-03-31/functions/function/invocations"
    
    response = requests.post(url, json={}, timeout=timeout)
    return response.json()


def test_successful_processing_with_valid_data(test_files):
    """Test 1: Successful Processing with Valid Data"""
    with create_lambda_container_with_env(test_files["valid_cohort"],  test_files["valid_gp_records"], tempfile.mkdtemp()) as container:

        result = invoke_lambda(container)

        #print(f"Lambda response: {result}")
        print("=== FULL RESPONSE ===")
        print(json.dumps(result, indent=2))   

        logs = container.get_logs()
        print("=== CONTAINER LOGS ===")
        print(logs)             

        assert result["statusCode"] == 200
        response_body = json.loads(result["body"])
        assert "records_processed" in response_body
        assert "records_retained" in response_body
        assert response_body["records_processed"] > 0


def test_successful_processing_with_empty_filtered_results(test_files):
    """Test 2: Successful Processing with Empty Filtered Results"""
    with create_lambda_container_with_env(test_files["empty_cohort"],  test_files["valid_gp_records"], tempfile.mkdtemp()) as container:

        result = invoke_lambda(container)
  
        assert result["statusCode"] == 200
        response_body = json.loads(result["body"])
        assert response_body["records_retained"] == 0


def test_successful_processing_with_no_input_records(test_files):
    """Test 3: Successful Processing with No Input Records"""
    with create_lambda_container_with_env(test_files["valid_cohort"],  test_files["empty_gp_records"], tempfile.mkdtemp()) as container:    
        result = invoke_lambda(container)
    
        assert result["statusCode"] == 200
        response_body = json.loads(result["body"])
        assert response_body["records_processed"] == 0