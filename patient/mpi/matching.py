# patient/mpi/matching_strategy.py
from abc import ABC, abstractmethod
from typing import List, Optional
import pandas as pd

class PatientMatchingStrategy(ABC):
    """Abstract base class for patient matching strategies."""
    
    @abstractmethod
    def find_matches(self, queries: pd.DataFrame) -> List[Optional[List[str]]]:
        """
        Find matching patients using this strategy.
        Strategy has full control - can query database directly or load into memory.
        
        Args:
            queries: DataFrame with search criteria (cleaned)
            
        Returns:
            List of lists of patient IDs (or None if no match)
        """
        pass

class SplinkMatchStrategy(PatientMatchingStrategy):
    """Patient matching using Splink probabilistic matching."""
    
    def find_matches(self, queries: pd.DataFrame) -> List[Optional[List[str]]]:
        return []  # TODO: implement Splink matching logic