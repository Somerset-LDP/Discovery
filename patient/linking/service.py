"""
Core Patient Linking Service.
Responsible for orchestrating matching logic and interacting with MPI.
"""
from datetime import date
from enum import Enum
from typing import Optional
import re

class Sex(Enum):
    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    UNKNOWN = "unknown"

class LinkageService:

    UK_POSTCODE_PATTERN = re.compile(
        r'^([A-Z]{1,2}\d{1,2}[A-Z]?)\s*(\d[A-Z]{2})$',
        re.IGNORECASE
    )    
    
    def link(self,         
             nhs_number: Optional[str] = None,
             dob: Optional[date] = None,
             postcode: Optional[str] = None,
             first_name: Optional[str] = None,
             last_name: Optional[str] = None,
             sex: Optional[Sex] = None):
        
        # Validate individual parameters
        if not self._is_valid_nhs_number(nhs_number):
            raise ValueError(f"Invalid NHS number format: {nhs_number}")
        
        if not self._is_valid_postcode(postcode):
            raise ValueError(f"Invalid UK postcode format: {postcode}")
        
        if not self._is_valid_name(first_name):
            raise ValueError("First name cannot be empty or whitespace")
        
        if not self._is_valid_name(last_name):
            raise ValueError("Last name cannot be empty or whitespace")
        
        if not self._is_valid_dob(dob):
            raise ValueError("Date of birth cannot be in the future")        
        
        # Check minimum parameter requirements
        has_nhs_and_dob = nhs_number is not None and dob is not None        
        if not (has_nhs_and_dob ):
            
            has_full_demographics = all([first_name is not None, last_name is not None, sex is not None, dob is not None, postcode is not None])
            if not has_full_demographics:
                
                raise ValueError(
                    "Insufficient data for patient linking. Required: "
                    "(nhs_number + dob) OR (first_name + last_name + sex + dob + postcode)"
                )
        # query local MPI for potential matches    
    
    # Validation helper methods
    
    def _is_valid_nhs_number(self, nhs_number: Optional[str]) -> bool:
        """
        Validates NHS number using Modulus 11 algorithm.
        Returns True if valid, False otherwise.
        """
        if nhs_number is None:
            return True  # None is valid (optional parameter)
        
        nhs_clean = str(nhs_number).replace(' ', '').strip()
        
        if not nhs_clean.isdigit() or len(nhs_clean) != 10:
            return False
        
        digits = [int(d) for d in nhs_clean]
        total = sum(d * (10 - i) for i, d in enumerate(digits[:9]))
        remainder = total % 11
        check_digit = 11 - remainder
        
        if check_digit == 11:
            check_digit = 0
        if check_digit == 10:
            return False
        
        return check_digit == digits[9]
    
    def _is_valid_postcode(self, postcode: Optional[str]) -> bool:
        """
        Validates UK postcode format.
        Returns True if valid, False otherwise.
        """
        if postcode is None:
            return True  # None is valid (optional parameter)
        
        if not isinstance(postcode, str) or not postcode.strip():
            return False
        
        return self.UK_POSTCODE_PATTERN.match(postcode.strip()) is not None
    
    def _is_valid_name(self, name: Optional[str]) -> bool:
        """
        Validates that a name is not empty or whitespace.
        Returns True if valid, False otherwise.
        """
        if name is None:
            return True  # None is valid (optional parameter)
        
        if not isinstance(name, str) or not name.strip():
            return False
        
        return True
    
    def _is_valid_dob(self, dob: Optional[date]) -> bool:
        """
        Validates that date of birth is not in the future.
        Returns True if valid, False otherwise.
        """
        if dob is None:
            return True  # None is valid (optional parameter)
        
        return dob <= date.today()

