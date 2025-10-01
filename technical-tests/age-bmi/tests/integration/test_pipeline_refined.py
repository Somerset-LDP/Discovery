from datetime import datetime
from decimal import Decimal
from importlib.abc import Traversable
import os
from pathlib import Path
import pytest
from testcontainers.postgres import PostgresContainer
from pipeline_refined import run_refined_pipeline
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.engine import Engine, URL
from typing import Generator, Optional, cast
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.network import Network
import requests
from fhirclient import client
import time
import stat
import shutil
import tempfile
from pathlib import Path
from importlib.resources import files, as_file
import traceback
import sys

valid_raw_patient = {
        "patient_id": 12345,
        "dob": "1985-06-15",
        "observations": [
            {
                "type": "Blood Pressure",
                "type_code_system": "LOINC",
                "value": 120,
                "unit": "mmHg",
                "observation_time": "2025-09-30T09:30:00Z"
            },
            {
                "type": "Heart Rate",
                "type_code_system": "SNOMED",
                "value": 72,
                "unit": "beats/min",
                "observation_time": "2025-09-30T09:35:00Z"
            }
        ]
    }

valid_refined_patient = {
        "patient_id": 12345,
        "dob": "1985-06-15",
        "height_cm": Decimal("180.50"),
        "height_observation_time": datetime.fromisoformat("2025-09-30T09:00:00"),
        "weight_kg": Decimal("75.20"),
        "weight_observation_time": datetime.fromisoformat("2025-09-30T09:05:00"),
        "bmi": Decimal("23.10"),
        "bmi_calculation_time": datetime.fromisoformat("2025-09-30T09:10:00")
    } 

@pytest.fixture(scope="session")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for containers to communicate."""
    network = Network()
    network.name = "ldp"
    network.create()  # type: ignore

    yield network

    network.remove()  # type: ignore

def wait_for_fhir_server(base_url: str, timeout: int = 120, interval: float = 5.0, container: Optional[DockerContainer] = None):
    """
    Wait until the FHIR server responds to /metadata or timeout.
    If container is provided, print logs while waiting.
    """
    start_time = time.time()
    metadata_url = f"{base_url}metadata"

    while True:
        try:
            resp = requests.get(metadata_url)
            if resp.status_code == 200:
                print(f"[DEBUG] FHIR server ready at {metadata_url}")
                return
        except requests.RequestException:
            pass

        if container:
            print("[DEBUG] Waiting for FHIR server to start...")
            #print(container.get_logs())

        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise RuntimeError(f"FHIR server at {base_url} did not start within {timeout} seconds")
        
        time.sleep(interval)

def execute_sql(sql_file: Traversable, conn: Connection):
    sql_file = files("data.init.ddl") / "schema_refined.sql"
    print(f"[DEBUG] Executing SQL file: {sql_file.name}")

    sql_text = sql_file.read_text()
    print(f"[DEBUG] SQL content preview (first 200 chars): {sql_text[:200]}...")        

    conn.execute(text(sql_text))    

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
def fhir_client(postgres_db: Engine, docker_network: Network) -> Generator[client.FHIRClient, None, None]:
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

            #.with_env("SPRING_DATASOURCE_USERNAME", "test") \
        #.with_env("SPRING_DATASOURCE_PASSWORD", "test")

    with DockerContainer("hapiproject/hapi:latest") \
        .with_network(docker_network) \
        .with_exposed_ports(8080) \
        .with_volume_mapping(str(local_config), container_config_path) as container:
        
        #time.sleep(60)
        #print("[DEBUG] HAPI FHIR server container started.")
        #print(container.get_logs())

        wait_for_logs(container, "Started Application", timeout=60)

        host_port = container.get_exposed_port(8080)
        base_url = f"http://localhost:{host_port}"

        #wait_for_fhir_server(base_url, container=container)
        print(f"[DEBUG] FHIR server will be accessible at {base_url}")

        settings = {
            "app_id": "fhir-server",
            "api_base": base_url
        }

        print("Initializing FHIRServer with api_base:", settings['api_base'])
        yield client.FHIRClient(settings=settings)

def test_run_pipeline(postgres_db: Engine, fhir_client: client.FHIRClient):
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
        assert actual["bmi_calculation_time"] == valid_refined_patient["bmi_calculation_time"]
