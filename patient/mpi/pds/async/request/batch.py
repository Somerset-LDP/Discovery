"""
Batch accumulator for PDS requests.
"""

class Batch:
    def __init__(self):
        self.items = []

    def add(self, patient_claim):
        # TODO: add item to batch
        self.items.append(patient_claim)

