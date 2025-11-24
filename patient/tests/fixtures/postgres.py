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
                result = conn.execute(text("SELECT datname FROM pg_database WHERE datname IN ('ldp');"))
                existing_dbs = [row[0] for row in result]
                
                assert "ldp" in existing_dbs, "LDP database not created"
                print("Verified that 'ldp' database exists.")
            
            default_engine.dispose()

            ldp_url = default_url.rsplit("/", 1)[0] + "/ldp?options=-c%20search_path=mpi,public"
            
            # Connect to LDP database to apply migrations
            ldp_admin_engine = create_engine(ldp_url)

            try:
                with ldp_admin_engine.connect() as ldp_conn:
                    # Apply migrations to LDP database
                    apply_migrations(ldp_conn, "mpi.local.data.migrations")
            finally:
                ldp_admin_engine.dispose()            

            # Connect to LDP database for tests
            ldp_engine = create_engine(ldp_url)

            try:
                yield ldp_engine
            finally:
                # Clean up tables between tests
                try:
                    with ldp_engine.connect() as conn:
                        conn.execute(text("TRUNCATE TABLE mpi.patient CASCADE"))
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

def apply_migrations(conn, resource_package: str = "mpi.local.data.migrations"):
    """Apply SQL migration files from a package resource in order."""
    try:
        with as_file(files(resource_package)) as migrations_path:
            print(f"Applying migrations from package: {resource_package}")

            migrations_dir = Path(migrations_path).resolve()
            migration_files = sorted(migrations_dir.glob("*.sql"))
            
            if not migration_files:
                print(f"No migration files found in {resource_package}")
                return
            
            for migration_file in migration_files:
                print(f"Applying migration: {migration_file.name}")
                sql_content = migration_file.read_text()
                conn.execute(text(sql_content))
                conn.commit()
                print(f"Applied: {migration_file.name}")
    except (ModuleNotFoundError, FileNotFoundError):
        print(f"Warning: Migrations package not found: {resource_package}")