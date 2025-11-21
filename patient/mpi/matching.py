from abc import ABC, abstractmethod
from typing import List
import pandas as pd

class PatientMatchingStrategy(ABC):
    @abstractmethod
    def find_matches(self, queries: pd.DataFrame) -> List[List[str]]:
        """
        Find patient records matching the provided query data.
        
        Args:
            queries: DataFrame with patient search criteria
            
        Returns:
            List of lists of patient IDs. Empty list for no matches.
            Length must equal len(queries).
        """
        pass

class SplinkMatchStrategy(PatientMatchingStrategy):
    """Patient matching using Splink probabilistic matching."""
    
    def find_matches(self, queries: pd.DataFrame) -> List[List[str]]:
        return []  # TODO: implement Splink matching logic