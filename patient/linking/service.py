"""
Core Patient Linking Service.
Responsible for orchestrating matching logic and interacting with MPI.
"""
from datetime import date
from enum import Enum
from typing import Optional
import re
from mpi.local.repository import PatientRepository
from mpi.pds.asynchronous.request.client import add_to_batch
from .patient import clean_patient, mark_unverified
import pandas as pd



class LinkageService:

    def __init__(self, local_mpi: PatientRepository):   
        self.local_mpi = local_mpi
    
    def link(self, df: pd.DataFrame):
        """Finds potential matches for patients in the given DataFrame. The original data frame is modified to add a 'patient_ids' column that contains lists of matching patient IDs or None if no match is found."""
                
        if df.empty:
            raise ValueError("DataFrame is empty")
        
        # validate and standardise input data
        clean_patient(df)
                    
        # query local MPI for potential matches  
        # this will use the default matching strategy (SQL exact match)
        patient_ids = self.local_mpi.find_patients(df)
        df['patient_ids'] = patient_ids

        # Find rows with no local matches and create new unverified patients for them
        self._create_unverified_patients(df[df['patient_ids'].isna()])

    def _create_unverified_patients(self, unmatched: pd.DataFrame):
        if len(unmatched) > 0:    

            # add to PDS async batch request
            batch_id = add_to_batch(unmatched)

            # create new unverified Patient in local MPI
            mark_unverified(unmatched)
            patient_ids = self.local_mpi.save(unmatched)
            unmatched['patient_ids'] = [[pid] for pid in patient_ids] # patient_ids is a 2D array on the DF

            # TODO - we **may** need to associate patient ids back to original df i.e. the patient_ids column