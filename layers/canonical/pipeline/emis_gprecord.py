import pandas as pd
from typing import Any, List, Dict, Callable
import logging

expected_columns = [
    ('Patient Details', 'NHS Number'),
    ('Patient Details', 'Given Name'),
    ('Patient Details', 'Family Name'),
    ('Patient Details', 'Date of Birth'),
    ('Patient Details', 'Postcode'),
    ('Patient Details', 'Gender'),
    ('Height', 'Value'),
    ('Height', 'Unit of Measure'),
    ('Height', 'Date'),
    ('Weight', 'Value'),
    ('Weight', 'Unit of Measure'),
    ('Weight', 'Date'),
    ('Consultations', 'Consultation ID'),
    ('Consultations', 'Date'),
    ('Consultations', 'Type of Consultation'),
    ("Consultations", "User Details' User Type")
]

# we need our lambda to convert the rows into a  dict
def run(records: pd.DataFrame) -> pd.DataFrame:
    #df = pd.read_csv("path/to/file.csv", header=[0, 1])
   
    canonical_records = []

    logger = logging.getLogger(__name__)

    if list(records.columns) != expected_columns:
        logger.warning(f"Unexpected columns found: {list(records.columns)}")
        raise ValueError("Input data does not match expected EMIS GP Record structure")

    logger.info(f"Starting GP pipeline processing for {len(records)} records")

    for index, record in records.iterrows():
        logger.debug(f"Processing record at index {index}")

        canonical_record = _to_canonical(record)
        if canonical_record:
            canonical_records.append(canonical_record)
        else:
            logger.warning(f"Record at index {index} failed validation and will be skipped")

    return _to_dataframe(canonical_records)

def _to_canonical(record: pd.Series) -> Dict[str, Any] | None:
    canonical_record = None
    
    patient_details = record["Patient Details"]
    height_data = record["Height"]
    weight_data = record["Weight"]

    if (_is_patient_details_valid(patient_details) and 
        _is_measurement_valid(height_data, "cm") and
        _is_measurement_valid(weight_data, "kg")):

        canonical_record = {
            'nhs_number': patient_details["NHS Number"],
            'given_name': patient_details["Given Name"],
            'family_name': patient_details["Family Name"],
            'date_of_birth': patient_details["Date of Birth"],
            'postcode': patient_details["Postcode"],
            'sex': patient_details["Gender"],
            'height_cm': height_data["Value"],
            'height_observation_time': height_data["Date"],
            'weight_kg': weight_data["Value"],
            'weight_observation_time': weight_data["Date"],
            }  

    return canonical_record  


# TODO - the function does not check that Gender is a valid member of the expected code set. 
# This will be tackled in a future iteration
def _is_patient_details_valid(patient_details: pd.Series) -> bool:
    logger = logging.getLogger(__name__)
    valid = False

    missing_mask = patient_details.isna() | (patient_details.astype(str).str.strip() == "")
    if missing_mask.any():
        logger.warning(f"Missing values in Patient Details: {patient_details[missing_mask]}")
    else:
         valid = True

    return valid

def _is_measurement_valid(measurement: pd.Series, unit: str) -> bool:
    logger = logging.getLogger(__name__)
    
    # Check for missing values
    missing_mask = measurement.isna() | (measurement.astype(str).str.strip() == "")
    if missing_mask.any():
        logger.warning(f"Missing values in Measurement: {measurement[missing_mask]}")
        return False

    # Check that Unit of Measure contains the expected unit
    unit_value = measurement["Unit of Measure"]
    if unit_value != unit:
        logger.warning(f"Unit of Measure mismatch. Expected '{unit}', but found '{unit_value}'")
        return False
    
    # Check that the value is numeric
    value = measurement["Value"]
    try:
        numeric_value = pd.to_numeric(value, errors='raise')
        logger.debug(f"Valid numeric value found: {numeric_value}")
    except (ValueError, TypeError) as e:
        logger.warning(f"Non-numeric value found in Value column: '{value}' - {str(e)}")
        return False
    
    # Check that the date is a valid date
    date_value = measurement["Date"]
    try:
        pd.to_datetime(date_value, format='%d-%b-%y', errors='raise')
        logger.debug(f"Valid date found: {date_value}")
    except ValueError:
        logger.warning(f"Invalid date found: '{date_value}'")
        return False    

    return True

def _to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    logger = logging.getLogger(__name__)

    if records:
        df = pd.DataFrame(records)
        logger.info(f"Successfully converted {len(records)} records to DataFrame with {len(df.columns)} columns")
    else:
        # Return empty DataFrame with expected columns if no valid records
        df = pd.DataFrame(columns=[
            'nhs_number', 'given_name', 'family_name', 'date_of_birth', 
            'postcode', 'sex', 'height_cm', 'height_observation_time', 
            'weight_kg', 'weight_observation_time'
        ])
        logger.warning("No valid records found - returning empty DataFrame")

    return df
