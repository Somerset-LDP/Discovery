import os
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
import logging
from fhirclient import client
from fhir.diagnostic_service import DiagnosticsService
from fhir.terminology_service import TerminologyService
from sqlalchemy import create_engine

logging.basicConfig(
    filename='logs/pipeline_refined_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

snomed_coding_system = "http://snomed.info/sct"
uom_coding_system = "http://unitsofmeasure.org"

def map_to_snomed(type_code, type_code_system, fhir_client: Optional[client.FHIRClient] = None):
    """
    Use ConceptMap to map input code/system to SNOMED-CT code.
    Ensures the resulting code system is SNOMED-CT.
    """
    snomed_coding = TerminologyService.translate(code=type_code, system=type_code_system, client=fhir_client)
    if snomed_coding and snomed_coding.system == snomed_coding_system:
        return snomed_coding
    else:
        logging.warning(f"ConceptMap translation did not return SNOMED-CT code for {type_code} ({type_code_system}).")
        return None

def convert_value_to_standard_unit(value, input_unit, standard_unit, standard_unit_system):
    """
    Convert value from input_unit to standard_unit.
    For demo, assume units are compatible and conversion is 1:1.
    Extend with actual conversion logic as needed.
    """
    if standard_unit_system != uom_coding_system:
        raise ValueError(f"Unsupported standard unit system: {standard_unit_system}. Must be {uom_coding_system}.")


    # TODO: Implement real unit conversion logic
    return value    

def standardise_observation(obs, fhir_client: Optional[client.FHIRClient] = None):
    snomed_code = map_to_snomed(obs["type"], obs["type_code_system"], fhir_client)
    if not snomed_code: 
        logging.warning(f"Could not map observation type '{obs['type']}' ({obs['type_code_system']}) to SNOMED-CT.")
        return None

    if not snomed_code.code:
        logging.warning(f"Mapped SNOMED-CT coding has no 'code' property: {snomed_code}")
        return None
    
    obs_def = DiagnosticsService.get_observation_definition(snomed_code.code, snomed_coding_system, fhir_client)
    if not obs_def or not obs_def.quantitativeDetails or not obs_def.quantitativeDetails.unit:
        logging.warning(f"No valid ObservationDefinition or permitted units found for SNOMED-CT code '{snomed_code}'.")
        return None

    standard_unit_code = obs_def.quantitativeDetails.unit.coding[0].code
    standard_unit_system = obs_def.quantitativeDetails.unit.coding[0].system
    standard_value = convert_value_to_standard_unit(obs["value"], obs["unit"], standard_unit_code, standard_unit_system)

    # Create a new standardised observation dict
    standardised_obs = {
        "type": snomed_code.code,  # original type
        "type_code_system": snomed_code.system,  # should be SNOMED-CT
        "value": standard_value,
        "unit": standard_unit_code,
        "observation_time": obs["observation_time"]
    }

    return standardised_obs

def standardise_patient_observations(raw_patients, fhir_client: Optional[client.FHIRClient] = None):
    enriched_patients = []
    for patient in raw_patients:
        # Copy patient to avoid mutating input
        patient_copy = dict(patient)
        patient_copy["observations"] = list(patient.get("observations", []))  # ensure it's a list
        for obs in patient.get("observations", []):
            standardised_obs = standardise_observation(obs, fhir_client)
            if standardised_obs:
                patient_copy["observations"].append(standardised_obs)
        enriched_patients.append(patient_copy)
    return enriched_patients

def write_refined_patients(output_df, engine=None):
    """
    Store the output DataFrame to a relational database table.
    """
    if engine is None:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise EnvironmentError("Environment variable DATABASE_URL must be set.")

        engine = create_engine(db_url)

    print(f"[DEBUG] Writing refined patients to database using engine: {engine} with DATABASE_URL={os.getenv('DATABASE_URL')}")

    try:
        output_df.to_sql("patient", engine, if_exists="append", index=False, schema="refined")
    except Exception as e:
        logging.error(f"Failed to write refined patients to database: {e}", exc_info=True)
        print(f"[ERROR] Failed to write refined patients to database: {e}")
        raise

def transform_to_refined_patients(patients):
    # Read SNOMED codes for height and weight from environment variables
    SNOMED_BODY_HEIGHT = os.getenv("SNOMED_BODY_HEIGHT")
    SNOMED_BODY_WEIGHT = os.getenv("SNOMED_BODY_WEIGHT")

    if not SNOMED_BODY_HEIGHT or not SNOMED_BODY_WEIGHT:
        raise EnvironmentError("Environment variables SNOMED_BODY_HEIGHT and SNOMED_BODY_WEIGHT must be set.")    

    output_rows = []
    for patient in patients:
        height_cm = None
        height_observation_time = None
        weight_kg = None
        weight_observation_time = None

        dob = patient.get("dob")
        patient_id = patient.get("patient_id")

        for obs in patient.get("observations", []):
            if obs.get("type_code_system") == snomed_coding_system:
                if obs.get("type") == SNOMED_BODY_HEIGHT:
                    height_cm = obs.get("value")
                    height_observation_time = obs.get("observation_time")
                elif obs.get("type") == SNOMED_BODY_WEIGHT:
                    weight_kg = obs.get("value")
                    weight_observation_time = obs.get("observation_time")

        now = pd.Timestamp.now()

        output_rows.append({
            "patient_id": patient_id,
            "dob": pd.to_datetime(dob).date() if dob else None,
            "height_cm": height_cm,
            "height_observation_time": height_observation_time,
            "weight_kg": weight_kg,
            "weight_observation_time": weight_observation_time,
            "ethnicity_code": patient.get("ethnicity", {}).get("code"),
            "ethnicity_code_system": patient.get("ethnicity", {}).get("system"),
            "sex_code": patient.get("sex", {}).get("code"),
            "sex_code_system": patient.get("sex", {}).get("system"),
            "created_at": now, # TODO - upsert logic required
            "updated_at": now
        })

    return pd.DataFrame(output_rows)    

def read_raw_patients(path="datalake/raw_patients.parquet"):
    df = pd.read_parquet(path)
    return df.to_dict(orient="records") 

def run_refined_pipeline(raw_patients, refined_store=None, fhir_client: Optional[client.FHIRClient] = None):
    # TODO - this first step now needs to read the unprocseed records from the pseudonymised layer - raw and enriched
    #raw_patients = read_raw_patients()
    enriched_patients = standardise_patient_observations(raw_patients, fhir_client)
    output_df = transform_to_refined_patients(enriched_patients)
    write_refined_patients(output_df, refined_store)
