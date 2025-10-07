import time
import subprocess
from typing import Generator

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy import text

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.network import Network

from importlib.resources import files, as_file
from fhirclient import client

@pytest.fixture(scope="session")
def fhir_container(postgres_db: Engine, docker_network: Network) -> Generator[str, None, None]:
    """Set up HAPI FHIR server container."""
    # Wait for HAPI database
    with postgres_db.connect() as conn:
        while True:
            result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname='hapi'")).fetchone()
            if result:
                print("[DEBUG] HAPI database exists, starting FHIR container")
                break
            print("[DEBUG] Waiting for HAPI database...")
            time.sleep(1)

    local_config = files("fhir") / "hapi.application.yaml"
    container_config_path = "/app/config/application.yaml"

    with DockerContainer("hapiproject/hapi:latest") \
        .with_network(docker_network) \
        .with_exposed_ports(8080) \
        .with_volume_mapping(str(local_config), container_config_path) as container:

        wait_for_logs(container, "Started Application", timeout=60)
        
        host_port = container.get_exposed_port(8080)
        base_url = f"http://localhost:{host_port}/fhir"
        
        print(f"[DEBUG] FHIR server accessible at {base_url}")
        yield base_url

@pytest.fixture(scope="session")
def load_fhir_resources(fhir_container) -> Generator[None, None, None]:
    """Load FHIR resources into the server."""
    base_url = fhir_container
    with as_file(files("data.fhir-store.resources") / "load_resources.sh") as script_path:
        subprocess.run([str(script_path), base_url], check=True)
    yield

@pytest.fixture(scope="session")
def fhir_client(fhir_container, load_fhir_resources) -> Generator[client.FHIRClient, None, None]:
    """Create configured FHIR client."""
    base_url = fhir_container
    
    settings = {
        "app_id": "fhir-server",
        "api_base": base_url
    }
    
    print("Initializing FHIRClient with api_base:", settings['api_base'])
    yield client.FHIRClient(settings=settings)