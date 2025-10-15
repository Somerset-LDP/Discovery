import shutil
import tempfile
import traceback
import sys
from pathlib import Path
from typing import Generator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from testcontainers.postgres import PostgresContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.core.network import Network

from importlib.resources import files, as_file

def postgres_init_dir(resource_package: str = "data.init") -> Path:
    """Create a host-controlled temp directory for Postgres init scripts."""
    with as_file(files(resource_package)) as temp_source_dir:
        temp_source_path = Path(temp_source_dir).resolve()
        host_temp_dir = Path(tempfile.mkdtemp(prefix="pg_init_"))

        for item in temp_source_path.iterdir():
            target = host_temp_dir / item.name
            if item.is_file():
                shutil.copy(item, target)
            elif item.is_dir():
                shutil.copytree(item, target)

        for path in host_temp_dir.rglob("*"):
            path.chmod(0o755)
        host_temp_dir.chmod(0o755)

        print(f"Postgres init dir ready at: {host_temp_dir}")
        return host_temp_dir

@pytest.fixture(scope="session")
def postgres_db(docker_network: Network) -> Generator[Engine, None, None]:
    """Set up PostgreSQL container with LDP and HAPI databases."""
    init_dir = postgres_init_dir("data.init")

    try:
        with PostgresContainer(
            image="postgres:16", 
            port=5432, 
            username="admin", 
            password="admin", 
            dbname="admin"
        ) \
        .with_network(docker_network) \
        .with_network_aliases("db") \
        .with_volume_mapping(str(init_dir), "/docker-entrypoint-initdb.d") as postgres:

            wait_for_logs(postgres, "database system is ready to accept connections", timeout=30)

            # Verify database creation
            default_url = postgres.get_connection_url()
            default_engine = create_engine(default_url)
            
            with default_engine.connect() as conn:
                result = conn.execute(text("SELECT datname FROM pg_database WHERE datname IN ('hapi','ldp');"))
                existing_dbs = [row[0] for row in result]
                
                assert "hapi" in existing_dbs, "HAPI database not created"
                assert "ldp" in existing_dbs, "LDP database not created"
                print("Verified that 'hapi' and 'ldp' databases exist.")

            default_engine.dispose()

            # Connect to LDP database
            ldp_url = postgres.get_connection_url().rsplit("/", 1)[0] + "/ldp"
            ldp_engine = create_engine(ldp_url)

            try:
                yield ldp_engine
            finally:
                # Clean up tables between tests
                try:
                    with ldp_engine.connect() as conn:
                        conn.execute(text("TRUNCATE TABLE derived.patient CASCADE"))
                        conn.execute(text("TRUNCATE TABLE refined.patient CASCADE"))
                        conn.commit()
                except Exception as cleanup_error:
                    print(f"Warning: Failed to cleanup tables: {cleanup_error}")
                
                ldp_engine.dispose()
                shutil.rmtree(init_dir, ignore_errors=True)
                
    except Exception as e:
        print("Exception during PostgresContainer startup")
        traceback.print_exc(file=sys.stdout)
        shutil.rmtree(init_dir, ignore_errors=True)
        raise