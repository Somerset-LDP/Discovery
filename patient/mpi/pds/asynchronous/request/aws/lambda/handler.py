"""
Timer-triggered Lambda for submitting PDS batch requests.
Wraps client.submit_batch.
"""
from mpi.pds.async.request.client import submit_batch

def handler(event, context):
    return submit_batch()

