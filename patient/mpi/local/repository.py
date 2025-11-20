"""
Local MPI repository for patient identity storage and retrieval.
Abstracts local store (DynamoDB, SQL, etc.)
"""
from typing import List, Optional
from sqlalchemy import Engine, text, Connection
import pandas as pd
from .matching import SqlExactMatchStrategy
from ..matching import PatientMatchingStrategy

class PatientRepository:
    def __init__(self, engine: Engine, save_batch_size: int = 1000):
        self.engine = engine
        self.save_batch_size = save_batch_size
        self.default_matcher = SqlExactMatchStrategy(engine)
    
    def get(self, patient_id):
        # TODO: implement store lookup
        pass

    def save(self, patients: pd.DataFrame) -> List[str]:
        patient_ids = []

        if not patients.empty:
            with self.engine.begin() as conn:
                for start_idx in range(0, len(patients), self.save_batch_size):
                    batch = patients.iloc[start_idx:start_idx + self.save_batch_size]
                    patient_ids.extend(self._insert_patients(batch, conn))
        
        return patient_ids

    def find_patients(self, queries: pd.DataFrame, matcher: Optional[PatientMatchingStrategy] = None) -> List[Optional[List[str]]]:
        if queries.empty:
            return []
        
        if matcher is None:
            matcher = self.default_matcher

        return matcher.find_matches(queries)  

    def _insert_patients(self, patients: pd.DataFrame, conn: Connection) -> List[str]:
        """Insert a batch of patients and return their IDs."""
        
        result = conn.execute(text("""
            INSERT INTO canonical.patient (
                nhs_number, given_name, family_name,
                date_of_birth, postcode, sex,
                verified, created_at, updated_at
            )
            SELECT 
                unnest(:nhs_numbers::TEXT[]),
                unnest(:given_names::TEXT[]),
                unnest(:family_names::TEXT[]),
                unnest(:dobs::TEXT[]),
                unnest(:postcodes::TEXT[]),
                unnest(:sexes::TEXT[]),
                unnest(:verifieds::BOOLEAN[]),
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            RETURNING patient_id
        """), {
            'nhs_numbers': patients['nhs_number'].tolist(),
            'given_names': patients['first_name'].tolist(),
            'family_names': patients['last_name'].tolist(),
            'dobs': patients['dob'].astype(str).tolist(),
            'postcodes': patients['postcode'].tolist(),
            'sexes': patients['sex'].tolist(),
            'verifieds': patients['verified'].tolist()
        })
        
        return [row[0] for row in result.fetchall()]