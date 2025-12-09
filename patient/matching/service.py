"""
Core Patient Matching Service.
Responsible for orchestrating matching logic and interacting with MPI.
"""

from mpi.local.repository import PatientRepository
from .patient import clean_patient, mark_unverified
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MatchingService:

    def __init__(self, local_mpi: PatientRepository):   
        self.local_mpi = local_mpi
    
    def match(self, df: pd.DataFrame) -> pd.DataFrame:
        """Finds potential matches for patients in the given DataFrame. 
        The original data frame is modified to add a 'patient_ids' column that contains lists of 
        matching patient IDs or None if no match is found.
        Args:
            df (pd.DataFrame): DataFrame containing patient data to be matched.
        Returns:
            pd.DataFrame: The modified DataFrame with an additional 'patient_ids' column.
        """
                
        if df.empty:
            raise ValueError("DataFrame is empty")
        
        self._validate_schema(df)
        
        # Work on copy - preserve original
        working_df = df.copy()
        
        # validate and standardise input data
        clean_patient(working_df)
      
        # exclude rows which do not have sufficient data for searching
        is_searchable = self._find_searchable_rows(working_df)
        logger.debug(f"There are {is_searchable.sum()} rows out of {len(working_df)} total rows that can be used as input to the search.")
       
        self._local_search(working_df, is_searchable)

        # Only create unverified patients for searchable unmatched rows
        is_unmatched = is_searchable & working_df['patient_ids'].apply(lambda x: len(x) == 0)

        logger.debug(f"Creating unverified patients for {is_unmatched.sum()} unmatched rows.")
        self._create_unverified_patients(working_df, is_unmatched)

        return working_df
    
    def _create_unverified_patients(self, df: pd.DataFrame, is_unmatched: pd.Series):
        """Creates unverified patients for rows matching the mask and updates df in place."""
        
        if is_unmatched.any():
            unmatched = df[is_unmatched]
            
            # create new unverified Patient in local MPI
            mark_unverified(unmatched)
            # TODO - we must pseudonymise patient data before storing it
            patient_ids = self.local_mpi.save(unmatched)
            
            # Update the original DataFrame using the mask
            for idx, patient_id in zip(unmatched.index, patient_ids):
                df.at[idx, 'patient_ids'] = [patient_id]  # Wrap in list for consistency

            logger.debug(f"Created {len(patient_ids)} unverified patients.")
            if len(patient_ids) < 100:
                logger.debug(f"Unverified patient IDs created: {patient_ids}")
        
        # TODO - we **may** need to associate patient ids back to original df i.e. the patient_ids column

    def _find_searchable_rows(self, df: pd.DataFrame) -> pd.Series:
        """
        Returns a boolean Series indicating whether each row is valid based on:
        cross check trace: nhs_number + dob both present
        OR
        regular trace: dob + postcode + first_name + last_name + sex all present
        """

        cross_check_trace = ["nhs_number", "dob"]
        trace = ["dob", "postcode", "first_name", "last_name", "sex"]

        is_non_empty = lambda v: not (
            pd.isna(v) or
            (isinstance(v, str) and v.strip() == "")
        )        

        cross_check_trace_valid = df.apply(
            lambda row: all(is_non_empty(row[col]) for col in cross_check_trace),
            axis=1
        )

        trace_valid = df.apply(
            lambda row: all(is_non_empty(row[col]) for col in trace),
            axis=1
        )

        return cross_check_trace_valid | trace_valid
    
    def _local_search(self, df: pd.DataFrame, is_searchable: pd.Series):
        """Performs local MPI search for rows matching the mask and updates df in place."""

        # Initialize patient_ids column if it doesn't exist
        if 'patient_ids' not in df.columns:
            df['patient_ids'] = None

        # rows that do not have searchable data get empty list as patient_ids
        for idx in df[~is_searchable].index:
            df.at[idx, 'patient_ids'] = []
        
        if is_searchable.any():
            searchable_df = df[is_searchable].copy()
            
            # Ensure all required columns exist for matching strategy
            for col in ['nhs_number', 'dob', 'postcode', 'first_name', 'last_name', 'sex']:
                if col not in searchable_df.columns:
                    searchable_df[col] = None

            # this will use the default matching strategy (SQL exact match)            
            patient_ids = self.local_mpi.find_patients(searchable_df)
     
            # Update the original DataFrame row by row
            for idx, patient_id in zip(searchable_df.index, patient_ids):
                df.at[idx, 'patient_ids'] = patient_id

    def _validate_schema(self, df: pd.DataFrame) -> None:
        """Ensure the DataFrame contains all mandatory columns."""
        mandatory_columns = ['nhs_number', 'dob', 'postcode', 'first_name', 'last_name', 'sex']

        missing = [col for col in mandatory_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing mandatory columns: {missing}")