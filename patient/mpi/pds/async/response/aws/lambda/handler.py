"""
Timer-triggered Lambda for checking PDS async response inbox.
If a response is found, triggers a trace event.
"""
from mpi.pds.async.response.trace import Trace

def handler(event, context):
    trace = Trace()
    return trace.check_for_response()

