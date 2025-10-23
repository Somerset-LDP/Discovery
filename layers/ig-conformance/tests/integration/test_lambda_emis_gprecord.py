import json
import os
import pytest
from pathlib import Path
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
import requests
import tempfile
import shutil
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
    container.with_volume_mapping(output_location_path, "/output", "rw")  # read-write

    # Set environment variables at container creation
    container.with_env("COHORT_STORE", "file:///cohort_store.csv")
    container.with_env("INPUT_LOCATION", "file:///input.csv")
    container.with_env("OUTPUT_LOCATION", "file:///output")

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:
        # our handler expects the input file to be deletable, so we need to copy it into the container
        container_id = running_container.get_wrapped_container().id
        
        cp_command = f"docker cp {input_location_path} {container_id}:/input.csv"
        
        result = os.system(cp_command)
        if result != 0:
            raise Exception(f"Docker cp failed with exit code: {result}")
        
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

@contextmanager
def create_test_workspace(cohort_file_path, input_file_path):
    """
    Create a temporary workspace with copies of test files.
    
    Args:
        cohort_file_path: Path to original cohort file
        input_file_path: Path to original input file
        
    Yields:
        tuple: (cohort_copy_path, input_copy_path, output_dir_path)
    """
    # Create temporary directory
    temp_dir = tempfile.mkdtemp(prefix="lambda_test_")
    
    try:
        # Create copies of input files
        cohort_copy = os.path.join(temp_dir, "cohort_copy.csv")
        input_copy = os.path.join(temp_dir, "input_copy.csv")
        
        shutil.copy2(cohort_file_path, cohort_copy)
        shutil.copy2(input_file_path, input_copy)
        
        # Output directory (same temp dir)
        output_dir = temp_dir
        
        yield cohort_copy, input_copy, output_dir
        
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)

def _file_exists_in_container(container, file_path: str) -> bool:
    """
    Check if a file exists in the container.
    
    Args:
        container: The running container instance
        file_path: Path to the file inside the container
        
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        result = container.exec(f"ls -la {file_path}")
        return result.exit_code == 0
    except Exception:
        return False        


def test_successful_processing_with_valid_data(test_files):
    """Test 1: Successful Processing with Valid Data"""
    with create_test_workspace(test_files["valid_cohort"], test_files["valid_gp_records"]) as (cohort_copy, input_copy, output_dir):
        
        with create_lambda_container_with_env(cohort_copy, input_copy, output_dir) as container:

            assert _file_exists_in_container(container, "/input.csv"), "Input file should exist before processing"
            result = invoke_lambda(container)

            #print("=== FULL RESPONSE ===")
            #print(json.dumps(result, indent=2))   

            #logs = container.get_logs()
            #print("=== CONTAINER LOGS ===")
            #print(logs)             

            assert result["statusCode"] == 200

            response_body = json.loads(result["body"])
            assert "records_processed" in response_body
            assert "records_retained" in response_body
            assert "output_file" in response_body
            assert response_body["records_processed"] == 5, f"Expected 5 records processed, got {response_body['records_processed']}"
            assert response_body["records_retained"] == 3, f"Expected 3 records retained, got {response_body['records_retained']}"
            assert not _file_exists_in_container(container, "/input.csv"), "Input file should be deleted after processing"

def test_successful_processing_with_empty_filtered_results(test_files):
    """Test 2: Successful Processing with Empty Filtered Results"""
    with create_test_workspace(test_files["empty_cohort"], test_files["valid_gp_records"]) as (cohort_copy, input_copy, output_dir):
        
        with create_lambda_container_with_env(cohort_copy, input_copy, output_dir) as container:

            assert _file_exists_in_container(container, "/input.csv"), "Input file should exist before processing"
            result = invoke_lambda(container)

            assert result["statusCode"] == 500
            
            response_body = json.loads(result["body"])
            assert "records_retained" not in response_body
            assert "output_file" not in response_body
            assert "message" in response_body
            assert response_body["message"] == "GP pipeline execution failed: Cohort file appears to be empty"
            assert _file_exists_in_container(container, "/input.csv"), "Input file should be not deleted after unsuccessful processing"

def test_successful_processing_with_no_input_records(test_files):
    """Test 3: Successful Processing with No Input Records"""
    with create_test_workspace(test_files["valid_cohort"], test_files["empty_gp_records"]) as (cohort_copy, input_copy, output_dir):
        
        with create_lambda_container_with_env(cohort_copy, input_copy, output_dir) as container:

            assert _file_exists_in_container(container, "/input.csv"), "Input file should exist before processing"
            
            result = invoke_lambda(container)

            assert result["statusCode"] == 200
            
            response_body = json.loads(result["body"])
            assert "records_processed" in response_body
            assert "records_retained" in response_body
            assert "output_file" in response_body
            assert response_body["records_processed"] == 0, f"Expected 0 records processed, got {response_body['records_processed']}"
            assert not _file_exists_in_container(container, "/input.csv"), "Input file should be deleted after processing"

