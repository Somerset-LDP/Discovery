import pandas as pd
from typing import Any, List, Dict, Tuple
import logging
from pipeline.canonical_feed_config import FeedConfig, get_feed_config


def run(records: pd.DataFrame, feed_type: str) -> pd.DataFrame:
    canonical_records = {}  # Dict with nhs_number as key

    logger = logging.getLogger(__name__)

    feed_config = get_feed_config(feed_type)
    logger.info(f"Processing {feed_type.upper()} feed with config: {feed_config.feed_type}")

    # Calculate expected number of columns
    expected_columns = len(feed_config.db_columns) + len(feed_config.csv_auxiliary_columns)

    # Just check we have expected number of columns
    if len(records.columns) < expected_columns:
        logger.warning(f"Insufficient columns. Expected at least {expected_columns}, got {len(records.columns)}")
        raise ValueError(f"Input data does not have enough columns for {feed_type.upper()} feed structure")

    logger.info(f"Starting {feed_type.upper()} pipeline processing for {len(records)} records")

    for index, record in records.iterrows():
        logger.debug(f"Processing record at index {index}")

        canonical_record = _to_canonical(record, feed_config)
        if canonical_record:
            nhs_number = canonical_record['nhs_number']

            existing_record = canonical_records.get(nhs_number, None)
            if existing_record:
                if canonical_record != existing_record:
                    logger.warning(f"Conflicting record found at index {index}")
            else:
                canonical_records[nhs_number] = canonical_record
                logger.debug(f"Added new patient at index {index}")
        else:
            logger.warning(f"Record at index {index} failed validation and will be skipped")

    logger.info(f"Processed {len(records)} records, returning {len(canonical_records)} unique valid records")
    return _to_dataframe(list(canonical_records.values()), feed_config)

def _parse_record(record: pd.Series, feed_config: FeedConfig) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    logger = logging.getLogger(__name__)
    
    patient_details = {}
    height_data = {}
    weight_data = {}
    
    db_cols = feed_config.db_columns

    try:
        # Extract patient details using DB column positions
        patient_details = {
            'nhs_number': record.iloc[db_cols['nhs_number']],
            'given_name': record.iloc[db_cols['given_name']],
            'family_name': record.iloc[db_cols['family_name']],
            'date_of_birth': record.iloc[db_cols['date_of_birth']],
            'postcode': record.iloc[db_cols['postcode']],
            'sex': record.iloc[db_cols['sex']]
        }

        # Extract height/weight data only if feed has measurements
        if feed_config.validation_rules.get('has_measurements', False):
            height_data = {
                'height_cm': record.iloc[db_cols['height_cm']],
                'height_observation_time': record.iloc[db_cols['height_observation_time']]
            }
            
            weight_data = {
                'weight_kg': record.iloc[db_cols['weight_kg']],
                'weight_observation_time': record.iloc[db_cols['weight_observation_time']]
            }
    except IndexError as e:
        logger.error(f"Column index out of range while parsing {feed_config.feed_type.upper()} record: {e}")
        return ({}, {}, {})
    except KeyError as e:
        logger.error(f"Missing column mapping in {feed_config.feed_type.upper()} config: {e}")
        return ({}, {}, {})

    return (patient_details, height_data, weight_data)

def _to_canonical(record: pd.Series, feed_config: FeedConfig) -> Dict[str, Any] | None:
    logger = logging.getLogger(__name__)

    canonical_record = None

    # Extract patient details using canonical keys
    patient_details, height_data, weight_data = _parse_record(record, feed_config)

    # Check that patient details are present
    if not patient_details:
        logger.warning("Patient details dictionary is empty after parsing")
        return canonical_record
    
    # Validate patient details
    if not _is_patient_details_valid(patient_details, feed_config):
        return canonical_record
    
    # For feeds with measurements, validate them
    if feed_config.validation_rules.get('has_measurements', False):
        if not height_data or not weight_data:
            logger.warning("Measurement data dictionaries are empty after parsing")
            return canonical_record
        
        aux_cols = feed_config.csv_auxiliary_columns
        if not all((_is_measurement_valid(
                height_data['height_cm'], 
                height_data['height_observation_time'], 
                record.iloc[aux_cols['height_unit']],
                feed_config.validation_rules['height_unit'],
                feed_config
            ),
            _is_measurement_valid(
                weight_data['weight_kg'], 
                weight_data['weight_observation_time'], 
                record.iloc[aux_cols['weight_unit']],
                feed_config.validation_rules['weight_unit'],
                feed_config
            ))):
            return canonical_record

    # Apply transformations to patient_details
    # Union the dictionaries together
    if feed_config.validation_rules.get('has_measurements', False):
        canonical_record = {**patient_details, **height_data, **weight_data}
    else:
        canonical_record = patient_details

    return canonical_record

# TODO - the function has a hard-coded enum for sex (pseudonymised). This will be updated in a future iteration to use a FHIR ValueSet
# Note that much of a Patient's data is pseudonymised therefore validation is limited to making sure mandatory values are present
def _is_patient_details_valid(patient_details: Dict[str, Any], feed_config: FeedConfig) -> bool:
    logger = logging.getLogger(__name__)
    
    required_fields = feed_config.validation_rules['required_patient_fields']

    # Check for missing/empty values in required fields
    for field in required_fields:
        value = patient_details.get(field)
        if pd.isna(value) or str(value).strip() == "":
            logger.warning(f"Missing or empty value in Patient Details for required field '{field}'")
            return False
        
    logger.debug("All patient details validation checks passed")
    return True


def _is_measurement_valid(
        value: str,
        date: str,
        uom: str,
        expected_uom: str,
        feed_config: FeedConfig
) -> bool:
    logger = logging.getLogger(__name__)

    # Check if the value is empty/missing - this is allowed based on config
    is_value_empty = pd.isna(value) or str(value).strip() == ""
    
    if is_value_empty:
        if feed_config.validation_rules.get('allow_empty_measurements', True):
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
    
    # Check that Date is present and matches the expected format from config
    if pd.isna(date) or str(date).strip() == "":
        logger.warning("Date is missing but required when Value is present")
        return False    
    
    date_format = feed_config.validation_rules['valid_date_format']
    try:
        pd.to_datetime(date, format=date_format, errors='raise')
        logger.debug(f"Valid date found: {date}")
    except ValueError:
        logger.warning(f"Invalid date found: '{date}' (expected format: {date_format})")
        return False

    return True

def _to_dataframe(records: List[Dict[str, Any]], feed_config: FeedConfig) -> pd.DataFrame:
    logger = logging.getLogger(__name__)

    if records:
        df = pd.DataFrame(records)
        logger.info(f"Successfully converted {len(records)} records to DataFrame with {len(df.columns)} columns")
    else:
        # Return empty DataFrame with expected columns based on feed config
        base_columns = ['nhs_number', 'given_name', 'family_name', 'date_of_birth', 'postcode', 'sex']
        
        if feed_config.validation_rules.get('has_measurements', False):
            columns = base_columns + ['height_cm', 'height_observation_time', 'weight_kg', 'weight_observation_time']
        else:
            columns = base_columns
            
        df = pd.DataFrame(columns=columns)
        logger.warning("No valid records found - returning empty DataFrame")

    return df
