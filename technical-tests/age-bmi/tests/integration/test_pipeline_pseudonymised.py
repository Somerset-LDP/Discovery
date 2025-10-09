"""
Integration test for the pseudonymised pipeline.
Tests the pseudonymised layer functionality: input validation, directory structure creation, and JSON output.
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from pipeline_pseudonymised import run_pseudonymised_pipeline


# Test data - single patient for happy path testing
valid_raw_patient = [
    {
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
]

@pytest.fixture
def input_json_file():
    """Create a temporary JSON file with test patient data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(valid_raw_patient, f, indent=2)
        temp_file_path = f.name
    
    yield temp_file_path


@pytest.fixture 
def pseudonymised_store():
    """Create a temporary directory for pseudonymised storage."""
    temp_dir = tempfile.mkdtemp(prefix="pseudonymised_test_")
    yield temp_dir

def test_run_pipeline(input_json_file: str, pseudonymised_store: str):
    """
    Simple test that validates the pseudonymised pipeline creates the expected output.
    """
    
    # Determine expected output path (today's date structure)
    today = datetime.now()
    expected_pseudonymised_store_feed_now = Path(pseudonymised_store) / f"{today.year}" / f"{today.month:02d}" / f"{today.day:02d}"
    expected_pseudonymised_store_feed_now_raw = expected_pseudonymised_store_feed_now / "raw"
    expected_pseudonymised_store_feed_now_patients = expected_pseudonymised_store_feed_now_raw / "patients.json"
    
    # Check that the folder does not exist before running
    assert not expected_pseudonymised_store_feed_now.exists(), f"Date directory should not exist before pipeline runs: {expected_pseudonymised_store_feed_now}"
    
    # Run the pipeline
    actual_pseudonymised_store_feed_now = run_pseudonymised_pipeline(input_path=input_json_file, pseudonymised_store=pseudonymised_store)

    # Check that the result_path matches the expected directory
    assert actual_pseudonymised_store_feed_now == expected_pseudonymised_store_feed_now, f"Result path should match expected date directory: {actual_pseudonymised_store_feed_now} != {expected_pseudonymised_store_feed_now}"

    # Check that the folder now exists and contains the expected file
    assert expected_pseudonymised_store_feed_now.exists(), f"Date directory should exist after pipeline runs: {expected_pseudonymised_store_feed_now}"
    assert expected_pseudonymised_store_feed_now_raw.exists(), f"Raw directory should exist: {expected_pseudonymised_store_feed_now_raw}"
    assert expected_pseudonymised_store_feed_now_patients.exists(), f"Raw patients.json file should exist: {expected_pseudonymised_store_feed_now_patients}"
    
    # Validate that the content matches valid_raw_patient
    with open(expected_pseudonymised_store_feed_now_patients) as f:
        output_data = json.load(f)
    
    assert output_data == valid_raw_patient, f"Output data should match input data"