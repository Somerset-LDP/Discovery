import logging
import os

from asynchronous.request.service import PdsAsyncRequestService
from asynchronous.request.trace_status import DynamoDbTraceStatus

# runs on a timer to submit batches of outstanding patient trace requests to PDS via MESH

# passes dynamodb backed implementation of pds-trace-status repository to service layer


# Configure logging
logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")  
logger.setLevel(log_level.upper())

def lambda_handler(event, context):
    trace_status = DynamoDbTraceStatus()
    service = PdsAsyncRequestService(trace_status)
    service.submit()

    # TODO - return some status object?
