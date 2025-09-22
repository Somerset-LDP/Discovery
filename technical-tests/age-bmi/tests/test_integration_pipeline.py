import os
import pytest
from testcontainers.postgres import PostgresContainer
from pipeline import run_pipeline
from db import get_connection, run_create_tables

@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres = PostgresContainer("postgres:15")
    postgres.start()

    def remove_container():
        postgres.stop()

    os.environ["POSTGRES_HOST"] = postgres.get_container_host_ip()
    os.environ["POSTGRES_PORT"] = str(postgres.get_exposed_port(5432))
    os.environ["POSTGRES_USER"] = postgres.username
    os.environ["POSTGRES_PASSWORD"] = postgres.password
    os.environ["POSTGRES_DB"] = postgres.dbname

    # Run SQL script to create tables
    run_create_tables()

    yield postgres  # tests run here

    postgres.stop()    

def test_run_pipeline():
    valid_record = {
        'patient_id': '1',
        'dob': '1980-05-12',
        'height': '180',
        'height_unit': 'cm',
        'weight': '75',
        'weight_unit': 'kg',
        'observation_time': '2025-09-22T10:00:00Z'
    }

    run_pipeline([valid_record])

    connection = get_connection()
    cur = connection.cursor()
    cur.execute("SELECT COUNT(*) FROM patient;")
    row = cur.fetchone()
    assert row is not None, "No rows returned"
    assert row[0] == 1

    cur.execute("SELECT COUNT(*) FROM observations;")
    # height + weight (+ bmi)
    row = cur.fetchone()
    assert row is not None, "No rows returned"
    assert row[0] >= 2    

    cur.close()
    connection.close()
