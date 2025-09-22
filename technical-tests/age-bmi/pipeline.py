import os
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from db import get_connection, store_refined

# =========================
# Configure logging
# =========================
logging.basicConfig(filename='logs/ingestion_errors.log',
                    level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# =========================
# Valid units
# =========================
VALID_HEIGHT_UNITS = {'cm'}
VALID_WEIGHT_UNITS = {'kg'}

# =========================
# Section 1: Ingestion & Raw Store
# =========================
def store_raw(records, raw_store_file='data/raw_stored.json'):
    # Store raw data to file (lineage)
    with open(raw_store_file, 'w') as f:
        for record in records:
            f.write(json.dumps(record) + '\n')

    return records

# =========================
# Section 2: Read Raw & Validate
# =========================
def validate_record(record):
    """
    Validate a single record. Returns (is_valid: bool, errors: list)
    """
    errors = []

    if not record.get('height') or not record.get('height_unit'):
        errors.append("Missing height/unit")
    if not record.get('weight') or not record.get('weight_unit'):
        errors.append("Missing weight/unit")
    if record.get('height_unit') and record.get('height_unit') not in VALID_HEIGHT_UNITS:
        errors.append(f"Unknown height unit: {record.get('height_unit')}")
    if record.get('weight_unit') and record.get('weight_unit') not in VALID_WEIGHT_UNITS:
        errors.append(f"Unknown weight unit: {record.get('weight_unit')}")
    if not record.get('observation_time'):
        errors.append("Missing observation_time")

    is_valid = len(errors) == 0
    return is_valid, errors

def validate_records(records):
    valid = []
    rejected = []

    for record in records:
        is_valid, errors = validate_record(record)
        if is_valid:
            valid.append(record)
        else:
            record['errors'] = errors
            rejected.append(record)

    # Log rejected
    for record in rejected:
        logging.info(record)

    return valid


# =========================
# Section 3: Transform Raw into Refined
# =========================
def convert_height(value, unit):
    if unit == 'cm':
        return float(value)
    return None

def convert_weight(value, unit):
    if unit == 'kg':
        return float(value)
    return None

def calculate_bmi(height_cm, weight_kg):
    if height_cm and weight_kg:
        return round(weight_kg / (height_cm/100)**2, 2)
    return None

def transform_records(records):
    for r in records:
        r['height_cm'] = convert_height(r['height'], r['height_unit'])
        r['weight_kg'] = convert_weight(r['weight'], r['weight_unit'])
        r['bmi'] = calculate_bmi(r['height_cm'], r['weight_kg'])
    return records

# pipeline.py

def run_pipeline(records):
    """
    Run the full pipeline given a list of deserialized records.
    If conn is provided, use it for storing refined data.
    """
    records = store_raw(records)

    # Section 2: Validate
    valid_records = validate_records(records)

    # Section 3: Transform
    transformed_records = transform_records(valid_records)

    # Section 4: Store refined
    store_refined(transformed_records)

    return transformed_records


# =========================
# Main Orchestration
# =========================
if __name__ == "__main__":
    file_path = 'data/patient_mock.csv'

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        records = [row for row in reader]    

    run_pipeline(records)
