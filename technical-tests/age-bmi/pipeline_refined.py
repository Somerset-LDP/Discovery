import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from fhir import DiagnosticsService, TerminologyService

logging.basicConfig(
    filename='logs/pipeline_refined_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

def map_to_snomed(type_code, type_code_system):
    """
    Use ConceptMap to map input code/system to SNOMED-CT code.
    Ensures the resulting code system is SNOMED-CT.
    """
    snomed_coding = TerminologyService.translate(code=type_code, system=type_code_system)
    if snomed_coding and snomed_coding.system == "http://snomed.info/sct":
        return snomed_coding
    else:
        logging.warning(f"ConceptMap translation did not return SNOMED-CT code for {type_code} ({type_code_system}).")
        return None  
    
def convert_value_to_standard_unit(value, input_unit, standard_unit):
    """
    Convert value from input_unit to standard_unit.
    For demo, assume units are compatible and conversion is 1:1.
    Extend with actual conversion logic as needed.
    """
    # TODO: Implement real unit conversion logic
    return value    

def standardise_observation(obs):
    snomed_code = map_to_snomed(obs["type"], obs["type_code_system"])
    if not snomed_code: 
        logging.warning(f"Could not map observation type '{obs['type']}' ({obs['type_code_system']}) to SNOMED-CT.")
        return None

    if not snomed_code.code:
        logging.warning(f"Mapped SNOMED-CT coding has no 'code' property: {snomed_code}")
        return None
    
    obs_def = DiagnosticsService.get_observation_definition(snomed_code.code)
    if not obs_def or not obs_def.quantitativeDetails or not obs_def.quantitativeDetails.permittedUnits:
        logging.warning(f"No valid ObservationDefinition or permitted units found for SNOMED-CT code '{snomed_code}'.")
        return None

    standard_unit = obs_def.quantitativeDetails.permittedUnits[0].code
    standard_value = convert_value_to_standard_unit(obs["value"], obs["unit"], standard_unit)

    # Create a new standardised observation dict
    standardised_obs = {
        "type": snomed_code.code,  # original type
        "type_code_system": snomed_code.system,  # should be SNOMED-CT
        "value": standard_value,
        "unit": standard_unit,
        "observation_time": obs["observation_time"]
    }

    return standardised_obs

def standardise_patient_observations(raw_patients):
    enriched_patients = []
    for patient in raw_patients:
        # Copy patient to avoid mutating input
        patient_copy = dict(patient)
        patient_copy["observations"] = list(patient.get("observations", []))  # ensure it's a list
        for obs in patient.get("observations", []):
            standardised_obs = standardise_observation(obs)
            if standardised_obs:
                patient_copy["observations"].append(standardised_obs)
        enriched_patients.append(patient_copy)
    return enriched_patients

def write_refined_patients(output_df):
    """
    Store the output DataFrame to a relational database table.
    """
    db_url = os.getenv("DATABASE_URL")

    if db_url is None:
      raise EnvironmentError("Environment variable DATABASE_URL must be set.")   
    table_name = "patient"  
    engine = create_engine(db_url)
    output_df.to_sql(table_name, engine, if_exists="append", index=False)

def transform_to_refined_patients(patients):
    # Read SNOMED codes for height and weight from environment variables
    SNOMED_BODY_HEIGHT = os.getenv("SNOMED_BODY_HEIGHT")
    SNOMED_BODY_WEIGHT = os.getenv("SNOMED_BODY_WEIGHT")

    if not SNOMED_BODY_HEIGHT or not SNOMED_BODY_WEIGHT:
        raise EnvironmentError("Environment variables SNOMED_BODY_HEIGHT and SNOMED_BODY_WEIGHT must be set.")    

    output_rows = []
    for patient in patients:
        height_cm = None
        weight_kg = None
        dob = patient.get("dob")
        patient_id = patient.get("patient_id")

        for obs in patient.get("observations", []):
            if obs.get("type_code_system") == "http://snomed.info/sct":
                if obs.get("type") == SNOMED_BODY_HEIGHT:
                    height_cm = obs.get("value")
                elif obs.get("type") == SNOMED_BODY_WEIGHT:
                    weight_kg = obs.get("value")

        bmi = None
        if height_cm and weight_kg:
            bmi = weight_kg / ((height_cm / 100) ** 2)

        output_rows.append({
            "patient_id": patient_id,
            "dob": pd.to_datetime(dob).date() if dob else None,
            "height_cm": height_cm,
            "weight_kg": weight_kg,
            "bmi": bmi
        })

    return pd.DataFrame(output_rows)    

def read_raw_patients(path="datalake/raw_patients.parquet"):
    df = pd.read_parquet(path)
    return df.to_dict(orient="records") 

def run_():
    raw_patients = read_raw_patients()
    enriched_patients = standardise_patient_observations(raw_patients)
    output_df = transform_to_refined_patients(enriched_patients)
    write_refined_patients(output_df)
