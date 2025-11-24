"""
Core Patient Matching Service.
Responsible for orchestrating matching logic and interacting with MPI.
"""

from mpi.local.repository import PatientRepository
from mpi.pds.asynchronous.request.client import add_to_batch
from .patient import clean_patient, mark_unverified
import pandas as pd



class MatchingService:

    def __init__(self, local_mpi: PatientRepository):   
        self.local_mpi = local_mpi
    
    def match(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finds potential matches for patients in the given DataFrame. The original data frame is modified to add a 'patient_ids' column that contains lists of matching patient IDs or None if no match is found."""
                
        if df.empty:
            raise ValueError("DataFrame is empty")
        
        # Work on copy - preserve original
        working_df = df.copy()
        
        # validate and standardise input data
        clean_patient(working_df)
      
        # exclude rows with no identifying data
        is_searchable = self._find_searchable_rows(working_df)
       
        self._local_search(working_df, is_searchable)

        # Only create unverified patients for searchable unmatched rows
        is_unmatched = is_searchable & working_df['patient_ids'].apply(lambda x: len(x) == 0)
        self._create_unverified_patients(working_df, is_unmatched)

        return working_df
    
    def _create_unverified_patients(self, df: pd.DataFrame, is_unmatched: pd.Series):
        """Creates unverified patients for rows matching the mask and updates df in place."""
        
        if is_unmatched.any():
            unmatched = df[is_unmatched]
            
            # add to PDS async batch request
            batch_id = add_to_batch(unmatched)

            # create new unverified Patient in local MPI
            mark_unverified(unmatched)
            patient_ids = self.local_mpi.save(unmatched)
            
            # Update the original DataFrame using the mask
            for idx, patient_id in zip(unmatched.index, patient_ids):
                df.at[idx, 'patient_ids'] = [patient_id]  # Wrap in list for consistency
        
        # TODO - we **may** need to associate patient ids back to original df i.e. the patient_ids column

    def _find_searchable_rows(self, df: pd.DataFrame) -> pd.Series:
        """Returns boolean mask indicating which rows have at least one non-None field."""
        return (
            df['nhs_number'].notna() |
            df['dob'].notna() |
            df['postcode'].notna() |
            df['first_name'].notna() |
            df['last_name'].notna() |
            df['sex'].notna()
        )
    
    def _local_search(self, df: pd.DataFrame, is_searchable: pd.Series):
        """Performs local MPI search for rows matching the mask and updates df in place."""

        # Initialize patient_ids column if it doesn't exist
        if 'patient_ids' not in df.columns:
            df['patient_ids'] = None

        # rows that do not have searchable data get empty list as patient_ids
        for idx in df[~is_searchable].index:
            df.at[idx, 'patient_ids'] = []
        
        if is_searchable.any():
            searchable_df = df[is_searchable]

            # this will use the default matching strategy (SQL exact match)            
            patient_ids = self.local_mpi.find_patients(searchable_df)
            
            # Update the original DataFrame row by row
            for idx, patient_id in zip(searchable_df.index, patient_ids):
                df.at[idx, 'patient_ids'] = patient_id