"""
Verification service.
Consumes trace events and updates verification status of patient records.
"""

class VerificationService:
    def verify(self, event):
        # handler will pass in id's of patients that need tracing
        
        
        trace_id = event.get("traceId")
        # TODO: fetch trace result
        # TODO: update all unverified patients
        return {"processed_trace": trace_id}

