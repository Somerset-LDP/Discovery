"""
AWS Lambda entrypoint for async verification events.
Wraps verification.verifier.Verifier.
"""
from patient.verification.service import VerificationService

def handler(event, context):
    verifier = VerificationService()
    return verifier.process_event(event)

