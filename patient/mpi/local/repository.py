"""
Local MPI repository for patient identity storage and retrieval.
Abstracts local store (DynamoDB, SQL, etc.)
"""
from typing import List, Optional
from sqlalchemy import Engine, text
import pandas as pd
from .matching import SqlExactMatchStrategy
from ..matching import PatientMatchingStrategy

class PatientRepository:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.default_matcher = SqlExactMatchStrategy(engine)
    
    def get(self, patient_id):
        # TODO: implement store lookup
        pass

    def save(self, patient_record):
        # TODO: implement store save
        pass

    def find_patients(self, queries: pd.DataFrame, matcher: Optional[PatientMatchingStrategy] = None) -> List[Optional[List[str]]]:
        if queries.empty:
            return []
        
        if matcher is None:
            matcher = self.default_matcher

        return matcher.find_matches(queries)        
 