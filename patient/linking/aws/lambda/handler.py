"""
AWS Lambda entrypoint for synchronous Patient Linking requests.
Wraps linking.service.LinkageService.
"""
from linking.service import LinkageService

def handler(event, context):
    service = LinkageService()
    return service.handle(event)

