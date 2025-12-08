from typing import List
from datetime import datetime
from sqlalchemy import Engine, text, Connection

class TraceStatus:

    def __init__(self, engine: Engine):
        self.engine = engine   

    def find_untraced_patients(self, patient_ids: List[str]) -> List[str]:
        """
        Finds patient IDs that do not have a trace status record.
        :param patient_id: List of patient IDs to be checked.
        :return: List of patient IDs without trace status records.
        Note that this method uses a SQL UNNEST function which may not be supported by all databases.
        """
        untraced_patients = []

        if not patient_ids:
            return untraced_patients
        
        query = """
            SELECT pid
            FROM UNNEST(:patient_ids) AS pid
            WHERE pid NOT IN (
                SELECT patient_id FROM trace_status
            )
        """    

        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                {"patient_ids": patient_ids}
            )
            untraced_patients = [row[0] for row in result]            

        return untraced_patients

    def mark_submitted(self, patient_id: List[str], submitted_at: datetime) -> bool:
        """
        Adds trace requests to the store of submitted (but not completed) requests.
        Note that if an existing record is found for a patient_id, it will not be updated.
        :param patient_id: List of patient IDs to be added.
        :param submitted_at: Timestamp of submission.
        :return: True if records were added successfully, False otherwise.
        Note that this method uses a SQL UNNEST function which may not be supported by all databases.
        """
        if not patient_id:
            return True
        
        insert_query = """
            INSERT INTO trace_status (patient_id, submitted_at)
            SELECT pid, :submitted_at
            FROM UNNEST(:patient_ids) AS pid
            ON CONFLICT (patient_id) DO NOTHING
        """

        with self.engine.begin() as conn:
            conn.execute(
                text(insert_query),
                {
                    "patient_ids": patient_id,
                    "submitted_at": submitted_at
                }
            )
        
        return True


    def mark_completed(self, patient_id: List[str], completed_at: datetime) -> bool:
        """
        Adds trace requests to the store of completed (and therefore submitted) requests.
        :param patient_id: List of patient IDs to be added.
        :param completed_at: Timestamp of completion.
        :return: True if records were added successfully, False otherwise.
        """    
        if not patient_id:
            return True
        
        update_query = """
            UPDATE trace_status
            SET completion_date = :completed_at
            WHERE patient_id = ANY(:patient_ids)
        """

        with self.engine.begin() as conn:
            conn.execute(
                text(update_query),
                {
                    "patient_ids": patient_id,
                    "completed_at": completed_at
                }
            )
        
        return True
