from pathlib import Path
import pandas as pd
import json
from jsonschema import validate, ValidationError
import logging
from datetime import datetime
import os
from importlib.resources import files

logging.basicConfig(
    filename='logs/pipeline_pseudonymised_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def read_raw_input(input_file_path):
    patients = []

    # Load schema from the data.pseudonymised-store package
    schema_resource = files("data.pseudonymised-store") / "schema-inbound.json"
    with schema_resource.open() as f:
        schema = json.load(f)    

    # Load input data from file
    input_path = Path(input_file_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    with open(input_path) as f:
        raw_input = json.load(f)

    for idx, patient in enumerate(raw_input):
        try:
            validate(instance=patient, schema=schema)
            patients.append(patient)
        except ValidationError as e:
            logging.error(f"Validation error for record {idx}: {e}")  

    return patients

def write_pseudonymised_patients(raw_patients, calculated_patients, base_path):
    """
    Write pseudonymised patients following the storage convention:
    pseudonymised/{feed_name}/YYYY/MM/DD/raw/ and /calculated/
    
    Args:
        raw_patients: List of patients with PII removed but structure preserved
        calculated_patients: List of patients with derived values (age, etc.)
        base_path: Root path for pseudonymised storage
        feed_name: Name of the data feed (e.g., 'feed_a', 'gp_data', 'hospital_data')
    
    Returns:
        dict: Paths where raw and calculated data were written
    """
    # Create date-based directory structure
    today = datetime.now()
    date_path = Path(base_path) / f"{today.year}/{today.month:02d}/{today.day:02d}"
    
    # Create full paths following the convention
    raw_dir = date_path / "raw"
    calculated_dir = date_path / "calculated"
    
    # Ensure directories exist
    raw_dir.mkdir(parents=True, exist_ok=True)
    calculated_dir.mkdir(parents=True, exist_ok=True)
    
    # Write raw pseudonymised data
    raw_file_path = raw_dir / "patients.json"
    with open(raw_file_path, 'w') as f:
        json.dump(raw_patients, f, indent=2)
    
    # Write calculated pseudonymised data
    calculated_file_path = calculated_dir / "patients.json"
    with open(calculated_file_path, 'w') as f:
        json.dump(calculated_patients, f, indent=2)
    
    return date_path

def create_calculated_patients(raw_patients):
    """
    Create calculated pseudonymised data (e.g., age from DOB).
    This is a placeholder for future implementation.
    
    Args:
        raw_patients: List of raw pseudonymised patients
        
    Returns:
        List of patients with calculated fields added
    """
    # TODO: Implement actual calculations (age from DOB, etc.)
    # For now, just return the same data
    calculated_patients = []
    for patient in raw_patients:
        calculated_patient = patient.copy()
        # TODO: Add calculated fields like age, derived demographics
        # calculated_patient['age'] = calculate_age_from_dob(patient.get('dob'))
        calculated_patients.append(calculated_patient)
    
    return calculated_patients

def run_pseudonymised_pipeline(input_path, pseudonymised_store):
    """
    Process raw patient data from input file and write to pseudonymised storage.
    Follows the storage convention: pseudonymised/{feed_name}/YYYY/MM/DD/{raw|calculated}/
    
    Args:
        input_file_path: Path to input JSON file containing raw patient data
        output_base_path: Root path for pseudonymised storage (e.g., "pseudonymised/")
        feed_name: Name of the data feed (e.g., 'gp_data', 'hospital_data')
        
    Returns:
        dict: Paths where raw and calculated pseudonymised data were written
    """
    # Read and validate input data
    raw_patients = read_raw_input(input_path)

    # TODO - call out to sub pipelines:
    # - pipeline_pseudonymised_raw.py: PII removal while preserving structure  
    # - pipeline_pseudonymised_enriched.py: PII-dependent calculations (age from DOB)
    
    # For now, create placeholder calculated data
    calculated_patients = create_calculated_patients(raw_patients)

    # Write to pseudonymised storage following the directory convention
    output_path = write_pseudonymised_patients(
        raw_patients=raw_patients,
        calculated_patients=calculated_patients, 
        base_path=pseudonymised_store
    )
    
    return output_path
