"""
Core Patient Linking Service.
Responsible for orchestrating matching logic and interacting with MPI.
"""
from datetime import date
from enum import Enum
from typing import Optional
import re
from mpi.local.repository import PatientRepository
from .patient import clean_patient
import pandas as pd



class LinkageService:

    def __init__(self, local_mpi: PatientRepository):   
        self.local_mpi = local_mpi
    
    def link(self, df: pd.DataFrame):
                
        if df.empty:
            raise ValueError("DataFrame is empty")
        
        # validate and standardise input data
        clean_patient(df)
                    
        # query local MPI for potential matches  
        # this will use the default matching strategy (SQL exact match)
        patient_ids = self.local_mpi.find_patients(df)
