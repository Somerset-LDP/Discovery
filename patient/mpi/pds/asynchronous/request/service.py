"""
Client responsible for submitting async PDS batch requests.
"""

from datetime import datetime, timezone
from mpi.pds.asynchronous.request.trace_status import TraceStatus
from mpi.local.repository import PatientRepository
import pandas as pd
import logging
from typing import TypedDict, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SubmitStatus(TypedDict):
    patient_ids: List[int]
    submission_time: Optional[datetime]

class PdsAsyncRequestService:

    def __init__(self, trace_status: TraceStatus, mpi: PatientRepository):
        self.trace_status = trace_status
        self.mpi = mpi

    def submit(self) -> SubmitStatus:
        """Submits unverified and untraced patients to PDS asynchronously via MESH.
        Note that duplicate patient_ids are dropped from submission as it is not clear which record to use.
        Returns:
            dict: A dictionary containing 'patient_ids' (list of submitted patient IDs) and 
                  'submission_time' (datetime of submission).
        """

        submission_time = None
        patient_ids = []
        
        unverified_patients = self.mpi.find_unverified_patients()
        logger.debug(f"Found {len(unverified_patients)} unverified patients")

        untraced_patients = self.trace_status.find_untraced_patients(unverified_patients["patient_id"].tolist())
        logger.debug(f"Found {len(untraced_patients)} untraced patients")

        # Retain full patient records for unverified and untraced patients
        unverified_untraced_patients = self._find_unique_untraced_patients(unverified_patients, untraced_patients)
        logger.debug(f"Found {len(unverified_untraced_patients)} unique unverified and untraced patients")

        # Filter for valid mesh rows
        valid_unverified_untraced_patients = self._find_valid_mesh_rows(unverified_untraced_patients)
        logger.debug(f"{len(valid_unverified_untraced_patients)} patients are valid for MESH submission")

        if not valid_unverified_untraced_patients.empty:        
            mesh_request = self._create_mesh_request(valid_unverified_untraced_patients) 
            logger.info(f"Submitting {len(mesh_request)} patients to PDS MESH")    
    
            # submit the batch to MESH
            submission_time = datetime.now()
            patient_ids = mesh_request["UNIQUE REFERENCE"].tolist()
            # TODO        


            self.trace_status.mark_submitted(untraced_patients, submission_time)
            logger.info(f"Marked {len(patient_ids)} patients as submitted at {submission_time.isoformat()}")

        # we might need a way to handle persistent failures here? perhaps lots of old submission dates and no completion dates
        # TODO
        
        return {
            "patient_ids": patient_ids,
            "submission_time": submission_time
        } 

    def _find_unique_untraced_patients(self, unverified_patients: pd.DataFrame, untraced_patient_ids: list) -> pd.DataFrame:    
        # Retain full patient records for unverified and untraced patients
        unverified_untraced_patients = unverified_patients[
            unverified_patients["patient_id"].isin(untraced_patient_ids)
        ]   

        # Drop all records with duplicate patient_id
        duplicates = unverified_untraced_patients["patient_id"].duplicated(keep=False)
        if duplicates.any():
            dropped_ids = unverified_untraced_patients.loc[duplicates, "patient_id"].unique()
            logging.warning(f"Dropping records with duplicate patient_ids: {dropped_ids.tolist()}")
            unverified_untraced_patients = unverified_untraced_patients[~duplicates] 

        return unverified_untraced_patients
   
    def _find_valid_mesh_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        nhs_trace = ["patient_id", "nhs_number", "date_of_birth"]
        fallback_trace = ["patient_id", "family_name", "given_name", "sex", "postcode", "date_of_birth"]

        def is_non_empty(val):
            return not (pd.isna(val) or (isinstance(val, str) and val.strip() == ""))

        nhs_valid = df.apply(lambda row: all(is_non_empty(row.get(col)) for col in nhs_trace), axis=1)
        fallback_valid = df.apply(lambda row: all(is_non_empty(row.get(col)) for col in fallback_trace), axis=1)

        valid_mask = nhs_valid | fallback_valid

        dropped_rows = df[~valid_mask]      
        if not dropped_rows.empty:
            dropped_ids = dropped_rows["patient_id"].tolist()
            logger.info(f"Dropping rows with patient_id(s) due to missing required fields: {dropped_ids}")          

        return df[valid_mask]

    def _create_mesh_request(self, patients: pd.DataFrame):
        """Creates a MESH batch request from the given patients DataFrame."""

        # Create a DataFrame with all required columns, filling missing ones with None
        # Column names as per MESH specification (https://digital.nhs.uk/developer/api-catalogue/personal-demographic-service-mesh/pds-mesh-data-dictionary#request-file-format)
        columns = [
            "UNIQUE REFERENCE", "NHS_NO", "FAMILY_NAME", "GIVEN_NAME", "OTHER_GIVEN_NAME", "GENDER",
            "DATE_OF_BIRTH", "POSTCODE", "DATE_OF_DEATH", "ADDRESS_LINE1", "ADDRESS_LINE2",
            "ADDRESS_LINE3", "ADDRESS_LINE4", "ADDRESS_LINE5", "ADDRESS_DATE", "GP_PRACTICE_CODE",
            "NHAIS_POSTING_ID", "AS_AT_DATE", "LOCAL_PATIENT_ID", "INTERNAL_ID", "TELEPHONE_NUMBER",
            "MOBILE_NUMBER", "EMAIL_ADDRESS"
        ]

        mesh_request = pd.DataFrame(columns=columns)
        mesh_request["UNIQUE REFERENCE"] = patients["patient_id"]
        mesh_request["NHS_NO"] = patients["nhs_number"]
        mesh_request["FAMILY_NAME"] = patients["family_name"]
        mesh_request["GIVEN_NAME"] = patients["given_name"]
        mesh_request["GENDER"] = patients["sex"]
        mesh_request["DATE_OF_BIRTH"] = patients["date_of_birth"]
        mesh_request["POSTCODE"] = patients["postcode"]

        return mesh_request

