import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, List, Dict, Tuple

logging.basicConfig(
    filename='logs/pipeline_derived_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Derived schema name
DERIVED_SCHEMA = os.getenv("DERIVED_SCHEMA", "derived")
REFINED_SCHEMA = os.getenv("REFINED_SCHEMA", "refined")
DB_URL = os.getenv("DATABASE_URL")

if DB_URL is None:
    raise EnvironmentError("Environment variable DB_URL must be set.")

engine = create_engine(DB_URL)

def read_refined_patients(changed_since: datetime) -> pd.DataFrame:
    """
    Fetch patients from the refined layer whose height or weight has changed
    since `changed_since`.
    """
    query = f"""
        SELECT *
        FROM {REFINED_SCHEMA}.patient
        WHERE height_observation_time > :since OR weight_observation_time > :since
    """
    return pd.read_sql(query, engine, params={"since": changed_since})

def calculate_bmi(height_cm: float, weight_kg: float) -> Optional[float]:
    if height_cm and weight_kg:
        return weight_kg / ((height_cm / 100) ** 2)
    return None

def adult_weight_category(bmi: float) -> str:
    """
    Determine adult weight category based on BMI.
    """
    if bmi is None:
        return None
    if bmi < 18.5:
        return "Underweight"
    elif bmi < 25:
        return "Normal"
    elif bmi < 30:
        return "Overweight"
    else:
        return "Obese"

def child_weight_category(bmi: float, age: int) -> str:
    """
    Determine child weight category. Example: simplified scheme.
    """
    if bmi is None or age is None:
        return None
    # Placeholder logic for demonstration
    if bmi < 14:
        return "Underweight"
    elif bmi < 18:
        return "Normal"
    else:
        return "Overweight"

def determine_weight_categories(patient_row: pd.Series, bmi: float) -> dict:
    """
    Apply applicable schemes and return a dict {scheme_name: category}.
    """
    categories = {}
    age = (datetime.utcnow().date() - patient_row["dob"]).days // 365

    if age >= 18:
        cat = adult_weight_category(bmi)
        if cat:
            categories["adult"] = cat
    else:
        cat = child_weight_category(bmi, age)
        if cat:
            categories["child"] = cat

    return categories

def write_patient_weight_categories(patient_id, categories: dict):
    rows = []
    now = datetime.utcnow()
    for scheme_name, category in categories.items():
        rows.append({
            "patient_id": patient_id,
            "scheme_name": scheme_name,
            "category": category,
            "calculation_time": now
        })
    df = pd.DataFrame(rows)
    df.to_sql("patient_weight_category", engine, schema=DERIVED_SCHEMA, if_exists="append", index=False)

def transform_to_derived_patients(patients_refined) -> Tuple[List[Dict], List[Dict]]:
    patients_derived = []
    patients_weight_categories = []

    # Compute BMI and weight categories
    for _, patient_refined in patients_refined.iterrows():
        bmi = calculate_bmi(patient_refined["height_cm"], patient_refined["weight_kg"])
        if bmi is None:
            continue

        patient_id = patient_refined["patient_id"]
        calc_time = datetime.utcnow()

        patients_derived.append({
            "patient_id": patient_id,
            "bmi": bmi,
            "bmi_calculation_time": calc_time
        })

        patient_weight_categories = determine_weight_categories(patient_refined, bmi)

        for scheme_name, category in patient_weight_categories.items():
            patients_weight_categories.append({
                "patient_id": patient_id,
                "scheme_name": scheme_name,
                "category": category,
                "calculation_time": calc_time
            })
    
    return patients_derived, patients_weight_categories

def write_derived_patients(patients_derived, patients_weight_categories):   
    # Write BMI and categories in a single transaction
    try:
        with engine.begin() as conn:
            if patients_derived:
                pd.DataFrame(patients_derived).to_sql("patient", conn, schema=DERIVED_SCHEMA, if_exists="append", index=False)

                if patients_weight_categories:
                    pd.DataFrame(patients_weight_categories).to_sql("patient_weight_category", conn, schema=DERIVED_SCHEMA, if_exists="append", index=False)
    except SQLAlchemyError as e:
        logging.error(f"Failed to write derived data: {e}")
        raise             

def run(changed_since: datetime):
    patients_refined = read_refined_patients(changed_since)
    patients_derived, patients_weight_categories = transform_to_derived_patients(patients_refined)
    write_derived_patients(patients_derived, patients_weight_categories)
 

