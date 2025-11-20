"""
Trace logic for identifying and persisting PDS async responses.
"""

class Trace:
    def __init__(self):
        # TODO: locate outstanding MESH response
        self.id = None

    def size(self):
        # TODO: return number of pending responses
        return 0

    def check_for_response(self):
        # TODO: detect and pseudonymise response
        # TODO: persist and publish event with trace ID
        return {"trace": "not_implemented"}

