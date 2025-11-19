# patient/mpi/strategies/sql_exact_match.py
from typing import List, Optional
from sqlalchemy import Engine, text
import pandas as pd
from mpi.matching import PatientMatchingStrategy

class SqlExactMatchStrategy(PatientMatchingStrategy):
    """Exact matching done entirely in SQL/PostgreSQL."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def find_matches(self, queries: pd.DataFrame) -> List[Optional[List[str]]]:
        """
        Find patient records matching the provided query data.
        
        Args:
            queries: DataFrame with patient search criteria (already cleaned by clean_patient).
                    Expected columns: nhs_number, dob, postcode, first_name, last_name, sex
            
        Returns:
            List of lists of internal patient IDs (or None if no match), same length as queries DataFrame.
            Each element is either:
            - None (no matches found)
            - List[str] (one or more patient IDs that matched)
        """
        if queries.empty:
            return []
        
        # Extract columns as lists - all columns guaranteed to exist
        row_indices = list(range(len(queries)))
        nhs_numbers = queries['nhs_number'].tolist()
        dobs = queries['dob'].astype(str).tolist()
        postcodes = queries['postcode'].tolist()
        first_names = queries['first_name'].tolist()
        last_names = queries['last_name'].tolist()
        sexes = queries['sex'].tolist()
        
        query = text("""
            WITH query_data AS (
                SELECT 
                    unnest(:row_indices::INTEGER[]) as row_idx,
                    unnest(:nhs_numbers::TEXT[]) as nhs_number,
                    unnest(:dobs::TEXT[]) as dob,
                    unnest(:postcodes::TEXT[]) as postcode,
                    unnest(:first_names::TEXT[]) as first_name,
                    unnest(:last_names::TEXT[]) as last_name,
                    unnest(:sexes::TEXT[]) as sex
            )
            SELECT 
                tqd.row_idx, 
                p.patient_id
            FROM query_data tqd
            LEFT JOIN canonical.patient p ON
                (tqd.nhs_number IS NULL OR p.nhs_number = tqd.nhs_number)
                AND (tqd.dob IS NULL OR p.date_of_birth = tqd.dob)
                AND (tqd.postcode IS NULL OR p.postcode = tqd.postcode)
                AND (tqd.first_name IS NULL OR p.given_name = tqd.first_name)
                AND (tqd.last_name IS NULL OR p.family_name = tqd.last_name)
                AND (tqd.sex IS NULL OR p.sex = tqd.sex)
            ORDER BY tqd.row_idx, p.patient_id
        """)
        
        with self.engine.connect() as conn:
            result_rows = conn.execute(query, {
                'row_indices': row_indices,
                'nhs_numbers': nhs_numbers,
                'dobs': dobs,
                'postcodes': postcodes,
                'first_names': first_names,
                'last_names': last_names,
                'sexes': sexes
            }).fetchall()
        
        # Group results by row index
        results: List[Optional[List[str]]] = [None] * len(queries)
        current_row_idx = None
        current_matches = []
        
        for row_idx, patient_id in result_rows:
            if row_idx != current_row_idx:
                # Save previous row's matches
                if current_row_idx is not None and current_matches:
                    results[current_row_idx] = current_matches
                # Start new row
                current_row_idx = row_idx
                current_matches = []
            
            # Add patient_id if not None (LEFT JOIN can return NULL)
            if patient_id is not None:
                current_matches.append(patient_id)
        
        # Don't forget the last row
        if current_row_idx is not None and current_matches:
            results[current_row_idx] = current_matches
        
        return results