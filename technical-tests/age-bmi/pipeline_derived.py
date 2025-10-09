import os
import logging
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List, Dict, Tuple, TypedDict
from rcpchgrowth import Measurement
from fhirclient import client
from calculators.bmi import calculate_bmi_and_category, Code

# Get a logger for this module
logger = logging.getLogger(__name__)

def read_refined_patients(changed_since: datetime, refined_store=None) -> pd.DataFrame:
    """
    Fetch patients from the refined layer whose height or weight has changed
    since `changed_since`.
    """
    if refined_store is None:
        db_url = os.getenv("REFINED_DATABASE_URL")
        if not db_url:
            raise EnvironmentError("Environment variable REFINED_DATABASE_URL must be set.")

        refined_store = create_engine(db_url)

    query = text("""
        SELECT *
        FROM refined.patient as patient
        WHERE patient.updated_at > :since
    """)

    return pd.read_sql(query, refined_store, params={"since": changed_since})

def transform_to_derived_patients(refined_patients, fhir_client: Optional[client.FHIRClient] = None) -> pd.DataFrame:
    print(f"Transforming {len(refined_patients)} refined patients to derived format.")
    
    patients_derived = []

    for _, refined_patient in refined_patients.iterrows():
        bmi, category = calculate_bmi_and_category(refined_patient, fhir_client)
        calc_time = datetime.utcnow()

        # age range?

        patients_derived.append({
            "patient_id": refined_patient["patient_id"],
            "bmi": bmi,
            "bmi_calculation_time": calc_time if bmi else None,
            "bmi_category": category["code"] if category else None,   
            "bmi_category_system": category["system"] if category else None, 
        })

    return pd.DataFrame(patients_derived)

def write_derived_patients(output_df, derived_store=None):
    """
    Store the output DataFrame to a relational database table.
    """
    if derived_store is None:
        db_url = os.getenv("DERIVED_DATABASE_URL")
        if not db_url:
            raise EnvironmentError("Environment variable DERIVED_DATABASE_URL must be set.")

        derived_store = create_engine(db_url)

    print(f"[DEBUG] Writing derived patients to database using engine: {derived_store} with DATABASE_URL={os.getenv('DERIVED_DATABASE_URL')}")

    try:
        output_df.to_sql("patient", derived_store, if_exists="append", index=False, schema="derived")
    except Exception as e:
        logger.error(f"Failed to write derived patients to database: {e}", exc_info=True)
        print(f"[ERROR] Failed to write derived patients to database: {e}")
        raise

def run_derived_pipeline(changed_since: datetime, refined_store=None, derived_store=None, fhir_client: Optional[client.FHIRClient] = None):
    patients_refined = read_refined_patients(changed_since, refined_store)
    patients_derived = transform_to_derived_patients(patients_refined, fhir_client)
    write_derived_patients(patients_derived, derived_store)
 

