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
        "ethnicity": {
            "code": "494131000000105",
            "system": "http://snomed.info/sct"
        },
        "sex": {
            "code": "248152002",
            "system": "http://snomed.info/sct"
        },        
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
        "ethnicity_code": "494131000000105",
        "ethnicity_code_system": "http://snomed.info/sct",
        "sex_code": "248152002",
        "sex_code_system": "http://snomed.info/sct"
    }

def test_run_pipeline(postgres_db: Engine, fhir_client: client.FHIRClient):
    # Set up environment variables for SNOMED codes  
    # These should match the SNOMED codes in the ConceptMap targets
    os.environ["SNOMED_BODY_HEIGHT"] = "50373000"  # Target SNOMED code for LOINC 8302-2
    os.environ["SNOMED_BODY_WEIGHT"] = "27113001"

    # Clean up any existing data from previous tests
    with postgres_db.connect() as conn:
        conn.execute(text("TRUNCATE TABLE derived.patient CASCADE"))
        conn.execute(text("TRUNCATE TABLE refined.patient CASCADE")) 
        conn.commit()
        
    with postgres_db.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        assert result == 0, f"Expected 0 patients after cleanup, found {result}"         

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
        assert actual["ethnicity_code"] == valid_refined_patient["ethnicity_code"]
        assert actual["ethnicity_code_system"] == valid_refined_patient["ethnicity_code_system"]
        assert actual["sex_code"] == valid_refined_patient["sex_code"]
        assert actual["sex_code_system"] == valid_refined_patient["sex_code_system"]
        assert actual["created_at"] is not None
        assert actual["updated_at"] is not None
