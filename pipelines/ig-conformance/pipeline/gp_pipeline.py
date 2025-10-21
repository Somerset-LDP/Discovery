import pandas as pd
from typing import Any, List, Dict
import logging
from common.cohort_membership import is_cohort_member

def run(cohort_store: pd.Series, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered_records = []
    
    logger = logging.getLogger(__name__)

    logger.info(f"Starting GP pipeline processing for {len(records)} records")
    logger.info(f"There are {len(cohort_store)} cohort members")

    for index, record in enumerate(records):
        logger.debug(f"Processing record at index {index}")
        nhs_number = record.get("nhs_number")
        if not nhs_number:
            logger.warning(f"Record at index {index} has no NHS number: {record}")
            continue

        # Pseudonymise the NHS number 
        # TODO - call pseudonymisation service here (speak to Barbara)
        # see implementation notes in README.md

        if is_cohort_member(nhs_number, cohort_store):
            ethnicity = record.get("ethnicity")
            if not ethnicity:
                logger.warning(f"Record at index {index} has no ethnicity")

            filtered_records.append(record)

    logger.info(f"Filtered {len(filtered_records)} records that are in cohort from an initial {len(records)} records")
    return filtered_records            