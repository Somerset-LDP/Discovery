from datetime import datetime
from decimal import Decimal
import os
import sys
import time
import shutil
import tempfile
import traceback
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Generator

from testcontainers.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.network import Network

from importlib.resources import files, as_file

from fhirclient import client
from pipeline_refined import run_refined_pipeline

valid_raw_patient = {
        "patient_id": 12345,
        "dob": "1985-06-15",
        "observations": [
            {
                "type": "8302-2",
                "type_code_system": "http://loinc.org",
                "value": 172,
                "unit": "cm",
                "observation_time": "2025-09-30T09:30:00Z"
            },
            {
                "type": "29463-7",
                "type_code_system": "http://loinc.org",
                "value": 68.5,
                "unit": "kg",
                "observation_time": "2025-09-30T09:35:00Z"
            }
        ]
    }

valid_refined_patient = {
        "patient_id": 12345,
        "dob": "1985-06-15",
        "height_cm": Decimal("172.00"),
        "height_observation_time": datetime.fromisoformat("2025-09-30T09:30:00"),
        "weight_kg": Decimal("68.50"),
        "weight_observation_time": datetime.fromisoformat("2025-09-30T09:35:00"),
        "bmi": Decimal("23.15"),
        #"bmi_calculation_time": datetime.fromisoformat("2025-09-30T09:10:00")
    } 

@pytest.fixture(scope="session")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for containers to communicate."""
    network = Network()
    network.name = "ldp"
    network.create()  # type: ignore

    yield network

    network.remove()  # type: ignore

def postgres_init_dir(resource_package: str = "data.init") -> Path:
    """
    Create a host-controlled temp directory for Postgres init scripts.
    Returns the path to the directory. Caller is responsible for cleanup.
    """
    with as_file(files(resource_package)) as temp_source_dir:
        temp_source_path = Path(temp_source_dir).resolve()

        # Create a persistent temp directory on the host
        host_temp_dir = Path(tempfile.mkdtemp(prefix="pg_init_"))

        # Copy files and make shell scripts executable
        for item in temp_source_path.iterdir():
            target = host_temp_dir / item.name
            if item.is_file():
                shutil.copy(item, target)
                #if target.suffix == ".sh":
                #    target.chmod(0o755)
            elif item.is_dir():
                shutil.copytree(item, target)

        # Recursively set permissions to 755
        for path in host_temp_dir.rglob("*"):
            path.chmod(0o755)
        host_temp_dir.chmod(0o755)                

        print(f"Postgres init dir ready at: {host_temp_dir}")
        return host_temp_dir

@pytest.fixture(scope="session")
def postgres_db(docker_network: Network) -> Generator[Engine, None, None]:
    
    # Create persistent host directory for init scripts
    init_dir = postgres_init_dir("data.init")

    try:
        # Start Postgres container
        with PostgresContainer(image="postgres:16", port=5432, username="admin", password="admin", dbname="admin") \
            .with_network(docker_network) \
            .with_network_aliases("db") \
            .with_volume_mapping(str(init_dir), "/docker-entrypoint-initdb.d") as postgres: 

            #print(postgres.get_logs())

            # Wait for Postgres to be ready
            wait_for_logs(postgres, "database system is ready to accept connections", timeout=30)     

            # Connect to the default "postgres" database to verify DB creation
            default_url = postgres.get_connection_url()
            default_engine = create_engine(default_url)
            with default_engine.connect() as conn:
                result = conn.execute(text("SELECT datname FROM pg_database WHERE datname IN ('hapi','ldp');"))
                existing_dbs = [row[0] for row in result]

                assert "hapi" in existing_dbs, "HAPI database not created by init scripts"
                assert "ldp" in existing_dbs, "LDP database not created by init scripts"
                print("Verified that 'hapi' and 'ldp' databases exist.")

            default_engine.dispose()         

            # Connect to the ldp database
            ldp_url = postgres.get_connection_url().rsplit("/", 1)[0] + "/ldp"
            ldp_engine = create_engine(ldp_url)

            # Yield engine connected to ldp for tests
            try:
                yield ldp_engine
            finally:
                ldp_engine.dispose()
                # Clean up the temp init directory
                shutil.rmtree(init_dir, ignore_errors=True)
                print(f"Cleaned up init dir: {init_dir}")
    except Exception as e:
        print("Exception during PostgresContainer startup")
        traceback.print_exc(file=sys.stdout)
        shutil.rmtree(init_dir, ignore_errors=True)
        raise     
    
@pytest.fixture(scope="session")
def fhir_container(postgres_db: Engine, docker_network: Network) -> Generator[str, None, None]:
    """
    Spins up a HAPI FHIR server container for integration tests.
    Returns the base URL for the FHIR server.
    """
    # Ensure the 'ldp' database exists and is ready
    with postgres_db.connect() as conn:
        while True:
            result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='hapi'")).fetchone()
            if result:
                print("[DEBUG] HAPI database exists, proceeding to start FHIR container")
                break
            print("[DEBUG] Waiting for HAPI database to be created...")
            time.sleep(1)

    # Mount your config if needed
    # Adjust the path to your local config file
    local_config = files("fhir") / "hapi.application.yaml"    
    container_config_path = "/app/config/application.yaml"

    with DockerContainer("hapiproject/hapi:latest") \
        .with_network(docker_network) \
        .with_exposed_ports(8080) \
        .with_volume_mapping(str(local_config), container_config_path) as container:

        wait_for_logs(container, "Started Application", timeout=60)

        host_port = container.get_exposed_port(8080)
        base_url = f"http://localhost:{host_port}/fhir"

        #wait_for_fhir_server(base_url, container=container)
        print(f"[DEBUG] FHIR server will be accessible at {base_url}")

        yield base_url

@pytest.fixture(scope="session")
def load_fhir_resources(fhir_container) -> Generator[None, None, None]:
    base_url = fhir_container
    with as_file(files("data.fhir-store.resources") / "load_resources.sh") as script_path:
        subprocess.run([str(script_path), base_url], check=True)
    yield  # No return value needed

@pytest.fixture(scope="session")
def fhir_client(fhir_container, load_fhir_resources) -> Generator[client.FHIRClient, None, None]:
    base_url = fhir_container

    settings = {
        "app_id": "fhir-server",
        "api_base": base_url
    }

    print("Initializing FHIRServer with api_base:", settings['api_base'])
    yield client.FHIRClient(settings=settings)               

def test_run_pipeline(postgres_db: Engine, fhir_client: client.FHIRClient):
    # Set up environment variables for SNOMED codes
    os.environ["SNOMED_BODY_HEIGHT"] = "248326004"
    os.environ["SNOMED_BODY_WEIGHT"] = "27113001"

    with postgres_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        assert result == 0, f"Expected 0 patients before insertion, found {result}"         

        run_refined_pipeline([valid_raw_patient], postgres_db, fhir_client)

        result = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        assert result == 1, f"Expected 1 patient after insertion, found {result}"

        actual = conn.execute(text("SELECT * FROM refined.patient")).mappings().fetchone()

        assert actual is not None
        assert actual["patient_id"] == valid_refined_patient["patient_id"]
        assert str(actual["dob"]) == valid_refined_patient["dob"]
        assert actual["height_cm"] == valid_refined_patient["height_cm"]
        assert actual["height_observation_time"] == valid_refined_patient["height_observation_time"]
        assert actual["weight_kg"] == valid_refined_patient["weight_kg"]
        assert actual["weight_observation_time"] == valid_refined_patient["weight_observation_time"]
        assert actual["bmi"] == valid_refined_patient["bmi"]
        assert abs((datetime.now() - actual["bmi_calculation_time"]).total_seconds()) < 60  # within 60 seconds