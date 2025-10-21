import json
import logging
import os
import pandas as pd
from typing import Any, List, Dict, cast
from pipeline.gp_pipeline import run
from common.cohort_membership import read_cohort_members

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda handler for GP pipeline processing
    
    Args:
        event: Lambda event data (could contain S3 event, manual trigger, etc.)
        context: Lambda context object
        
    Returns:
        dict: Response with status code and body
    """
    try:
        logger.info("Starting GP pipeline execution")
        logger.info(f"Event: {json.dumps(event, default=str)}")

        # read the cohort store
        cohort_store_location = os.getenv("COHORT_STORE")
        if not cohort_store_location:
            raise ValueError(f"COHORT_STORE env var not set")
        
        gp_records_store_location = os.getenv("GP_RECORDS_STORE")
        if not gp_records_store_location:
            raise ValueError(f"GP_RECORDS_STORE env var not set")

        # read the gp record file
        # TODO: implement reading GP records from S3


        result = run(read_cohort_members(cohort_store_location), _read_gp_records(gp_records_store_location))

        # TODO - delete the gp records as the LDP is not permitted to store raw data that holds plain text PII

        logger.info("GP pipeline executed successfully")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'GP pipeline executed successfully',
                'result': result,
                'requestId': context.aws_request_id
            })
        }
        
    except Exception as e:
        logger.error(f"GP pipeline execution failed: {str(e)}", exc_info=True)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'GP pipeline execution failed',
                'requestId': context.aws_request_id
            })
        }

def _read_gp_records(location: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(location, dtype={'nhs': str})

    # Ensure all column names are strings
    df.columns = df.columns.astype(str)    

    return cast(List[Dict[str, Any]], df.to_dict(orient='records'))