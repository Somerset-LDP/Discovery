from datetime import datetime, timedelta
from decimal import Decimal
import os
import sys
import shutil
import tempfile
import traceback
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

#from tests.fixtures import postgres_db, fhir_client, docker_network
from pipeline_derived import run_derived_pipeline

valid_refined_patient = {
        "patient_id": 12345,
        "dob": "1985-06-15",
        "height_cm": Decimal("75.0"),
        "height_observation_time": datetime.fromisoformat("2025-09-30T09:30:00"),
        "weight_kg": Decimal("9.50"),
        "weight_observation_time": datetime.fromisoformat("2025-09-30T09:35:00"),
        "ethnicity_code": "494131000000105",
        "ethnicity_code_system": "http://snomed.info/sct",
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct"
    } 
    
@pytest.fixture(scope="session")
def load_refined_patients(postgres_db) -> Generator[None, None, None]:
    # Load valid refined patient into the refined.patient table
    insert_stmt = text("""
        INSERT INTO refined.patient (
            patient_id,
            dob,
            height_cm,
            height_observation_time,
            weight_kg,
            weight_observation_time,
            sex_code,
            sex_code_system,
            ethnicity_code,
            ethnicity_code_system,
            created_at,
            updated_at
        ) VALUES (
            :patient_id,
            :dob,
            :height_cm,
            :height_observation_time,
            :weight_kg,
            :weight_observation_time,
            :sex_code,
            :sex_code_system,
            :ethnicity_code,
            :ethnicity_code_system,
            :created_at,
            :updated_at
        )
    """)

    with postgres_db.connect() as conn:
        try:
            conn.execute(
                insert_stmt,
                dict(
                    patient_id=valid_refined_patient["patient_id"],
                    dob=(datetime.now().date() - timedelta(days=365)), # we want to guarantee the patient is a 1 year old child
                    height_cm=valid_refined_patient["height_cm"],
                    height_observation_time=valid_refined_patient["height_observation_time"],
                    weight_kg=valid_refined_patient["weight_kg"],
                    weight_observation_time=valid_refined_patient["weight_observation_time"],
                    sex_code=valid_refined_patient["sex_code"],
                    sex_code_system=valid_refined_patient["sex_code_system"],
                    ethnicity_code=valid_refined_patient["ethnicity_code"],
                    ethnicity_code_system=valid_refined_patient["ethnicity_code_system"],
                    created_at=datetime.fromisoformat("2025-06-10T09:40:00"),
                    updated_at=datetime.fromisoformat("2025-06-10T09:40:00")
                )
            )
            conn.commit() 
            print("refined.patient inserted successfully.")
        except Exception as e:
            print(f"Error inserting refined.patient: {e}")
            import traceback
            traceback.print_exc()

    yield  # No return value needed

def test_run_pipeline(postgres_db: Engine, load_refined_patients, fhir_client):
    with postgres_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        assert result == 1, f"Expected 1 patient before insertion, found {result}"
        
        result = conn.execute(text("SELECT COUNT(*) FROM derived.patient")).scalar_one()
        assert result == 0, f"Expected 0 patients before insertion, found {result}" 

        # this date is before the refined patient's updated_at
        changed_since = datetime.fromisoformat("2025-05-10T09:40:00")
        run_derived_pipeline(changed_since, postgres_db, postgres_db, fhir_client)

        result = conn.execute(text("SELECT COUNT(*) FROM derived.patient")).scalar_one()
        assert result == 1, f"Expected 1 patient after insertion, found {result}"

        actual = conn.execute(text("SELECT * FROM derived.patient")).mappings().fetchone()

        print(f"Derived patient record: {actual}")

        assert actual is not None
        assert actual["patient_id"] == valid_refined_patient["patient_id"]
        assert actual["bmi"] == Decimal("16.89")  
        assert abs((datetime.utcnow() - actual["bmi_calculation_time"]).total_seconds()) < 60  # within 60 seconds
        assert actual["bmi_category"] == "162864001"
        assert actual["bmi_category_system"] == "http://snomed.info/sct"
        assert actual["created_at"] is not None
        assert actual["updated_at"] is not None