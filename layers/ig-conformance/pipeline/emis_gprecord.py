import pandas as pd
from typing import List, Dict, Callable
import logging

# Column position constants based on the header row
NHS_NUMBER_COL = 0

def run(cohort_store: pd.Series, records: pd.DataFrame, encrypt: Callable[[str, List[str]], List[str] | None]) -> pd.DataFrame:
    logger = logging.getLogger(__name__)

    logger.info(f"Starting GP pipeline processing for {len(records)} records")
    logger.info(f"There are {len(cohort_store)} cohort members")

    # Step 1: Extract and clean all NHS numbers, keeping track of valid records
    valid_records = []
    nhs_numbers_for_records = []
    
    for index, record in records.iterrows():
        nhs_number = record.iloc[NHS_NUMBER_COL]
        if not nhs_number or str(nhs_number).lower() in ['nan', 'none', 'null', '']:
            logger.warning(f"Record at index {index} has no NHS number: {record}")
            continue
            
        # Clean NHS number (remove spaces)
        cleaned_nhs = str(nhs_number).replace(' ', '').strip()
        valid_records.append(record)
        nhs_numbers_for_records.append(cleaned_nhs)
    
    logger.info(f"Found {len(valid_records)} valid records with NHS numbers")
    
    # Step 2: Get unique NHS numbers for batch encryption
    unique_nhs_numbers = list(set(nhs_numbers_for_records))
    logger.info(f"Found {len(unique_nhs_numbers)} unique NHS numbers from {len(nhs_numbers_for_records)} valid records")
    
    # Step 3: Batch encrypt all unique NHS numbers
    encrypted_mapping = _batch_encrypt_nhs_numbers(unique_nhs_numbers, encrypt)
    
    # Step 4: Filter records - keep ALL records where the NHS number is in cohort
    cohort_set = set()
    if not cohort_store.empty:
        # Clean and normalize cohort member NHS numbers once
        cohort_set = set(cohort_store.astype(str).str.strip().values)    
    
    filtered_records = []
    cohort_nhs_numbers = set()  # Track which NHS numbers are in cohort for logging

    for record, nhs_number in zip(valid_records, nhs_numbers_for_records):
        encrypted_nhs = encrypted_mapping.get(nhs_number)
        if encrypted_nhs:
            if encrypted_nhs.strip() in cohort_set:
                filtered_records.append(record)
                cohort_nhs_numbers.add(nhs_number)
            # Note: We don't log per-record here to avoid spam, we'll log summary below
        else:
            logger.error(f"Failed to encrypt NHS number: {nhs_number}")

    logger.info(f"Found {len(cohort_nhs_numbers)} unique NHS numbers in cohort")
    logger.info(f"Retained {len(filtered_records)} records (including duplicates) from an initial {len(records)} records")
    
    return pd.DataFrame(filtered_records)

def _batch_encrypt_nhs_numbers(nhs_numbers: List[str], encrypt: Callable[[str, List[str]], List[str] | None]) -> Dict[str, str]:
    """
    Encrypt a batch of NHS numbers and return a mapping from original to encrypted values.
    
    Args:
        nhs_numbers: List of unique NHS numbers to encrypt
        encrypt: Encryption function that supports batch operations
        
    Returns:
        Dict mapping original NHS numbers to encrypted values
    """
    logger = logging.getLogger(__name__)
    
    if not nhs_numbers:
        return {}
        
    logger.info(f"Batch encrypting {len(nhs_numbers)} unique NHS numbers")
    
    try:
        encrypted_values = encrypt("nhs_number", nhs_numbers)
        
        if encrypted_values and len(encrypted_values) == len(nhs_numbers):
            mapping = dict(zip(nhs_numbers, encrypted_values))
            logger.info(f"Successfully created encryption mapping for {len(mapping)} NHS numbers")
            return mapping
        else:
            logger.error(f"Batch encryption returned unexpected format: {type(encrypted_values)}, expected list of {len(nhs_numbers)} values")
            raise RuntimeError(f"Batch encryption failed: expected list of {len(nhs_numbers)} values, got {type(encrypted_values)}")
            
    except Exception as e:
        logger.error(f"Error in batch encryption: {e}")
        raise RuntimeError(f"Failed to process GP records due to batch encryption service error: {str(e)}")