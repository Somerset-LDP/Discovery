import pandas as pd
from typing import Any, List, Dict, Callable, Tuple
import logging

# Column position constants based on the header row
NHS_NUMBER_COL = 0
GIVEN_NAME_COL = 1
FAMILY_NAME_COL = 2
DATE_OF_BIRTH_COL = 3
POSTCODE_COL = 4
NUMBER_AND_STREET_COL = 5
GENDER_COL = 6
HEIGHT_VALUE_COL = 7
HEIGHT_UNIT_COL = 8
HEIGHT_DATE_COL = 9
WEIGHT_VALUE_COL = 10
WEIGHT_UNIT_COL = 11
WEIGHT_DATE_COL = 12
CONSULTATION_ID_COL = 13
CONSULTATION_DATE_COL = 14
CONSULTATION_TIME_COL = 15
CONSULTATION_TYPE_COL = 16
USER_TYPE_COL = 17

# we need our lambda to convert the rows into a  dict
def run(records: pd.DataFrame) -> pd.DataFrame:
    canonical_records = {}  # Dict with nhs_number as key

    logger = logging.getLogger(__name__)

    # Just check we expected number of columns (USER_TYPE_COL is the last column)
    if len(records.columns) < USER_TYPE_COL + 1:
        logger.warning(f"Insufficient columns. Expected at least {USER_TYPE_COL + 1}, got {len(records.columns)}")
        raise ValueError("Input data does not have enough columns for EMIS GP Record structure")    
    
    logger.info(f"Starting GP pipeline processing for {len(records)} records")

    for index, record in records.iterrows():
        logger.debug(f"Processing record at index {index}")

        canonical_record = _to_canonical(record)
        if canonical_record:
            nhs_number = canonical_record['nhs_number']

            existing_record = canonical_records.get(nhs_number, None)
            if existing_record:
                if canonical_record != existing_record:
                    logger.warning(f"Conflicting record found for NHS number {nhs_number} at index {index}")
            else:
                canonical_records[nhs_number] = canonical_record
                logger.debug(f"Added new patient with NHS number {nhs_number}")                
        else:
            logger.warning(f"Record at index {index} failed validation and will be skipped")

    logger.info(f"Processed {len(records)} records, returning {len(canonical_records)} unique valid records")
    return _to_dataframe(list(canonical_records.values()))

def _parse_record(record: pd.Series) -> Tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    logger = logging.getLogger(__name__)
    
    patient_details = {}
    height_data = {}
    weight_data = {}
    
    try:
        # Extract patient details using canonical keys
        patient_details = {
            'nhs_number': record.iloc[NHS_NUMBER_COL],
            'given_name': record.iloc[GIVEN_NAME_COL],
            'family_name': record.iloc[FAMILY_NAME_COL], 
            'date_of_birth': record.iloc[DATE_OF_BIRTH_COL],
            'postcode': record.iloc[POSTCODE_COL],
            'sex': record.iloc[GENDER_COL]
        }
        
        # Extract height data using canonical keys
        height_data = {
            'height_cm': record.iloc[HEIGHT_VALUE_COL],
            'height_observation_time': record.iloc[HEIGHT_DATE_COL]
        }
        
        # Extract weight data using canonical keys
        weight_data = {
            'weight_kg': record.iloc[WEIGHT_VALUE_COL],
            'weight_observation_time': record.iloc[WEIGHT_DATE_COL]
        }
    except IndexError as e:
        logger.error(f"Column index out of range: {e}")

    return (patient_details, height_data, weight_data)

def _to_canonical(record: pd.Series) -> Dict[str, Any] | None:
    logger = logging.getLogger(__name__)

    canonical_record = None
   
    # Extract patient details using canonical keys
    patient_details, height_data, weight_data = _parse_record(record)

    # Check that all dictionaries contain data
    if not patient_details or not height_data or not weight_data:
        logger.warning("One or more data dictionaries are empty after parsing")
        return canonical_record    

    if (_is_patient_details_valid(patient_details) and 
        _is_measurement_valid(height_data['height_cm'], height_data['height_observation_time'], record.iloc[HEIGHT_UNIT_COL], "cm") and
        _is_measurement_valid(weight_data['weight_kg'], weight_data['weight_observation_time'], record.iloc[WEIGHT_UNIT_COL], "kg")):

        # Apply transformations to patient_details
        # Remove spaces from NHS number
        patient_details['nhs_number'] = str(patient_details['nhs_number']).replace(' ', '')
        
        # Convert date of birth to proper date type
        #try:
        #    patient_details['date_of_birth'] = pd.to_datetime(patient_details['date_of_birth'], format='%d-%b-%y', errors='raise').date()
        #except ValueError as e:
        #    logger.error(f"Failed to convert date of birth '{patient_details['date_of_birth']}': {e}")
        #    return canonical_record

        # Union the dictionaries together
        canonical_record = {**patient_details, **height_data, **weight_data}  

    return canonical_record

# TODO - the function has a hard-coded enum for sex. This will be updated in a future iteration to use a FHIR ValueSet
# Note that much of a Patient's data is pseudonymised therefore validation is limited to making sure mandatory values are present
def _is_patient_details_valid(patient_details: Dict[str, Any]) -> bool:
    logger = logging.getLogger(__name__)
    
    # Check for missing/empty values in all fields
    for key, value in patient_details.items():
        if pd.isna(value) or str(value).strip() == "":
            logger.warning(f"Missing or empty value in Patient Details for field '{key}': '{value}'")
            return False
        
    logger.debug("All patient details validation checks passed")
    return True


def _is_measurement_valid(value: str, date: str, uom: str, expected_uom: str) -> bool:
    logger = logging.getLogger(__name__)

    # Check if the value is empty/missing - this is allowed
    is_value_empty = pd.isna(value) or str(value).strip() == ""
    
    if is_value_empty:
        logger.debug("Empty measurement value - this is valid")
        return True

    # Check that the value is numeric
    try:
        numeric_value = pd.to_numeric(value, errors='raise')
        logger.debug(f"Valid numeric value found: {numeric_value}")
    except (ValueError, TypeError) as e:
        logger.warning(f"Non-numeric value found in Value column: '{value}' - {str(e)}")
        return False

    # Check that Unit of Measure is present and matches expected unit
    if pd.isna(uom) or str(uom).strip() == "":
        logger.warning("Unit of Measure is missing but required when Value is present")
        return False
        
    if uom != expected_uom:
        logger.warning(f"Unit of Measure mismatch. Expected '{expected_uom}', but found '{uom}'")
        return False
    
    # Check that Date is present and matches the expected format
    if pd.isna(date) or str(date).strip() == "":
        logger.warning("Date is missing but required when Value is present")
        return False    
    
    try:
        pd.to_datetime(date, format='%d-%b-%y', errors='raise')
        logger.debug(f"Valid date found: {date}")
    except ValueError:
        logger.warning(f"Invalid date found: '{date}'")
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
