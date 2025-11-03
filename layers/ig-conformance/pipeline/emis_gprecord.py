import pandas as pd
from typing import Any, List, Dict, Callable
import logging
from common.cohort_membership import is_cohort_member

# Column position constants based on the header row
NHS_NUMBER_COL = 0

def run(cohort_store: pd.Series, records: pd.DataFrame, encrypt: Callable[[str, str], str | None]) -> pd.DataFrame:
    filtered_records = []
    
    logger = logging.getLogger(__name__)

    logger.info(f"Starting GP pipeline processing for {len(records)} records")
    logger.info(f"There are {len(cohort_store)} cohort members")

    for index, record in records.iterrows():
        logger.debug(f"Processing record at index {index}")
        nhs_number = record.iloc[NHS_NUMBER_COL]
        #nhs_number = record.get("nhs_number")
        if not nhs_number or str(nhs_number).lower() in ['nan', 'none', 'null', '']:
            logger.warning(f"Record at index {index} has no NHS number: {record}")
            continue

        encrypted_nhs = _encrypt_nhs_number(nhs_number, encrypt)

        if encrypted_nhs:
            if is_cohort_member(encrypted_nhs, cohort_store):
                filtered_records.append(record)
                logger.debug(f"NHS number at index {index} is in cohort")
            else:
                logger.debug(f"NHS number at index {index} is not in cohort")
        else:
            logger.error(f"Failed to encrypt NHS number for record at index {index}")

    logger.info(f"Filtered {len(filtered_records)} records that are in cohort from an initial {len(records)} records")
    return pd.DataFrame(filtered_records)            

def _encrypt_nhs_number(nhs_number: str, encrypt: Callable[[str, str], str | None]) -> str | None:
    encrypted_nhs = None
    
    logger = logging.getLogger(__name__)
    
    try:
        encrypted_nhs = encrypt("nhs_number", nhs_number)   
    except Exception as e:
        logger.error(f"Error encrypting NHS number: {e}")
        raise RuntimeError(f"Failed to process GP records due to encryption service error: {str(e)}")
    
    return encrypted_nhs