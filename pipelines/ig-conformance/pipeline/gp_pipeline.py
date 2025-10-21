import pandas as pd
from typing import List, Dict
import logging
from common.cohort_membership import is_cohort_member

def run(cohort_store: pd.Series, records: List[Dict[str, str]]):
    logger = logging.getLogger(__name__)

    logger.info(f"Starting GP pipeline processing for {len(records)} records")
    logger.info(f"There are {len(cohort_store)} cohort members")

    for index, record in enumerate(records):
        logger.debug(f"Processing record at index {index}")
        nhs_number = record.get("nhs_number")
        if not nhs_number:
            logger.warning(f"Record at index {index} has no NHS number: {record}")
            continue

        if is_cohort_member(nhs_number, cohort_store):
            ethnicity = record.get("ethnicity")
            if not ethnicity:
                logger.warning(f"Record at index {index} has no ethnicity")
                
            pass

# read GP data from raw S3 bucket
# extract NHS number
# check cohort membership
# if in cohort, 
#   extract Ethnicity
#   replace with synthetic data
# write modified GP data to IG conformant S3 bucket
# delete GP data from raw S3 bucket