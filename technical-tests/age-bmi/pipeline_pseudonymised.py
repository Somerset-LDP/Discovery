from pathlib import Path
import pandas as pd
import json
from jsonschema import validate, ValidationError
import logging

logging.basicConfig(
    filename='logs/pipeline_raw_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def read_raw_input(raw_input, schema_path="schema-inbound.json"):
    patients = []

    schema_path = Path(schema_path)
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
    with open(schema_path) as f:
        schema = json.load(f)    

    for idx, patient in enumerate(raw_input):
        try:
            validate(instance=patient, schema=schema)
            patients.append(patient)
        except ValidationError as e:
            logging.error(f"Validation error for record {idx}: {e}")  

    return patients

def write_raw_patients(raw_patients, path="datalake/raw_patients.parquet"):
    # Flatten each patient dict to a row for storage
    df = pd.json_normalize(raw_patients)
    df.to_parquet(path, index=False)
    # Alternatively, for JSON:
    # df.to_json("datalake/raw_patients.json", orient="records", lines=True)

def run(input_json):
    raw_patients = read_raw_input(input_json)

    # TODO - 
    # call out to sub pipelines 
    # - pipeline_pseudonymised_raw.py 
    # - pipeline_pseudonymised_enriched.py

    write_raw_patients(raw_patients)
