"""
Integration test for the complete Age-BMI pipeline.
Tests the happy path from raw input JSON through all three layers: pseudonymised -> refined -> derived.
"""

import json
import os
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Generator

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from fhirclient import client

from pipeline import run as run_complete_pipeline


# Test data - single patient with height and weight observations
valid_raw_patient = [
    {
        "patient_id": 98765,
        "dob": "1990-03-22", 
        "ethnicity": {
            "code": "494131000000105",  # White British
            "system": "http://snomed.info/sct"
        },
        "sex": {
            "code": "248152002",  # Female
            "system": "http://snomed.info/sct"
        },
        "observations": [
            {
                "type": "8302-2",  # Body height (LOINC)
                "type_code_system": "http://loinc.org",
                "value": 165,
                "unit": "cm",
                "observation_time": "2025-10-01T10:00:00Z"
            },
            {
                "type": "29463-7",  # Body weight (LOINC)
                "type_code_system": "http://loinc.org", 
                "value": 62.5,
                "unit": "kg",
                "observation_time": "2025-10-01T10:05:00Z"
            }
        ]
    }
]

@pytest.fixture
def input_json_file():
    """Create a temporary JSON file with test patient data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(valid_raw_patient, f, indent=2)
        temp_file_path = f.name
    
    yield Path(temp_file_path)


@pytest.fixture
def temp_pseudonymised_dir():
    """Create a temporary directory for pseudonymised storage."""
    temp_dir = tempfile.mkdtemp(prefix="test_pseudonymised_")
    yield Path(temp_dir)


def test_run_pipeline(input_json_file: Path, temp_pseudonymised_dir: Path, postgres_db: Engine, fhir_client: client.FHIRClient):
    """
    Test the complete pipeline from raw input to derived analytics - happy path.
    """
    # Set up environment variables required by the pipeline
    # These should match the SNOMED codes in the ConceptMap targets
    os.environ["SNOMED_BODY_HEIGHT"] = "50373000"  # Target SNOMED code for LOINC 8302-2 
    os.environ["SNOMED_BODY_WEIGHT"] = "27113001" 
    
    # Determine expected pseudonymised directory structure
    today = datetime.now()
    expected_pseudo_date_dir = Path(temp_pseudonymised_dir) / f"{today.year}" / f"{today.month:02d}" / f"{today.day:02d}"
    expected_pseudo_raw_dir = expected_pseudo_date_dir / "raw"
    expected_pseudo_raw_file = expected_pseudo_raw_dir / "patients.json"
    
    # Verify pseudonymised directory does not exist before test
    assert not expected_pseudo_date_dir.exists(), f"Pseudonymised date directory should not exist before pipeline runs: {expected_pseudo_date_dir}"
    
    # Verify database is empty before test
    with postgres_db.connect() as conn:
        refined_count = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        derived_count = conn.execute(text("SELECT COUNT(*) FROM derived.patient")).scalar_one()
        assert refined_count == 0, f"Expected 0 refined patients before test, found {refined_count}"
        assert derived_count == 0, f"Expected 0 derived patients before test, found {derived_count}"
    
    # Run the complete pipeline - for now the derived and refined databases sit in the same server
    run_complete_pipeline(input_json_file, temp_pseudonymised_dir, postgres_db, postgres_db, fhir_client)
    
    # Verify pseudonymised directory exists after test and contains expected content
    assert expected_pseudo_date_dir.exists(), f"Pseudonymised date directory should exist after pipeline runs: {expected_pseudo_date_dir}"
    assert expected_pseudo_raw_dir.exists(), f"Pseudonymised raw directory should exist: {expected_pseudo_raw_dir}"
    assert expected_pseudo_raw_file.exists(), f"Pseudonymised patients.json file should exist: {expected_pseudo_raw_file}"
    
    # Validate content of pseudonymised patients.json file
    with open(expected_pseudo_raw_file) as f:
        pseudo_data = json.load(f)
    
    assert pseudo_data == valid_raw_patient, f"Pseudonymised data should match input data"
    
    # Validate refined layer database content
    with postgres_db.connect() as conn:
        refined_count = conn.execute(text("SELECT COUNT(*) FROM refined.patient")).scalar_one()
        assert refined_count == 1, f"Expected 1 refined patient after pipeline, found {refined_count}"
        
        refined_patient = conn.execute(text("SELECT * FROM refined.patient")).mappings().fetchone()
        assert refined_patient is not None
        assert refined_patient["patient_id"] == 98765
        assert str(refined_patient["dob"]) == "1990-03-22"
        assert refined_patient["height_cm"] == Decimal("165.00")
        assert refined_patient["weight_kg"] == Decimal("62.50")
    
    # Validate derived layer database content
    with postgres_db.connect() as conn:
        derived_count = conn.execute(text("SELECT COUNT(*) FROM derived.patient")).scalar_one()
        assert derived_count == 1, f"Expected 1 derived patient after pipeline, found {derived_count}"
        
        derived_patient = conn.execute(text("SELECT * FROM derived.patient")).mappings().fetchone()
        assert derived_patient is not None
        assert derived_patient["patient_id"] == 98765
        
        # Validate BMI calculation (62.5 kg / (1.65 m)^2 ≈ 22.96)
        expected_bmi = 62.5 / (1.65 ** 2)
        assert abs(float(derived_patient["bmi"]) - expected_bmi) < 0.01, \
            f"BMI calculation incorrect. Expected ~{expected_bmi:.2f}, got {derived_patient['bmi']}"