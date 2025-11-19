"""
AWS Lambda entrypoint for async verification events.
Wraps verification.verifier.Verifier.
"""
from verification.verifier import Verifier

def handler(event, context):
    verifier = Verifier()
    return verifier.process_event(event)

