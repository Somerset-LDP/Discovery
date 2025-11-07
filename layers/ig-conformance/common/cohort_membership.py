import pandas as pd
from urllib.error import URLError
from common.filesystem import read_file
import logging

def read_cohort_members(location: str) -> pd.Series:
    """
    Get the NHS numbers of all cohort members.

    Args:
        location (str): Path or URL to the CSV file containing cohort member data.
                       Supports local files, HTTP/HTTPS URLs, and S3 URLs.
                       
                       Examples:
                       - Local: "file:///path/to/cohort.csv"
                       - HTTP: "https://example.com/cohort.csv"
                       - S3: "s3://bucket-name/path/to/cohort.csv"

    Returns:
        pd.Series: A series containing the NHS numbers of all cohort members.
        
    Raises:
        FileNotFoundError: If the cohort file does not exist.
        PermissionError: If the cohort file cannot be accessed.
        ValueError: If the file is empty or missing required 'nhs_number' column.
        pd.errors.EmptyDataError: If the CSV file is empty.
        pd.errors.ParserError: If the CSV file is malformed.
        ConnectionError: If unable to access remote location.
    """ 
    try:
        # Read CSV with nhs column as string to preserve leading zeros
        df = read_file(location)

        if df.empty:
            raise ValueError(f"No data found in cohort file")
        
        if df.shape[1] == 0:
            raise ValueError(f"No columns found in cohort file")        
          
        # Get nhs column and handle nulls/whitespace
        nhs_numbers = df.iloc[:, 0]
        
        # Filter out null, NaN, empty strings, and whitespace-only values
        # Since nhs is now string type, pd.isna() handles <NA> values properly
        valid_nhs = nhs_numbers[~pd.isna(nhs_numbers)]
        valid_nhs = valid_nhs[valid_nhs.str.strip() != '']
        valid_nhs = valid_nhs[valid_nhs.str.strip().str.lower() != 'nan']
        
        if valid_nhs.empty:
            raise ValueError(f"No valid NHS numbers found")
        
        # Strip whitespace from valid NHS numbers (they're already strings)
        valid_nhs = valid_nhs.str.strip()
                
        return valid_nhs
        
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"Cohort file appears to be empty")
    
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Error parsing cohort file")

    except (ConnectionError, TimeoutError) as e:
        raise ConnectionError(f"Unable to connect to location {location}: {str(e)}")

    except PermissionError as e:
        raise PermissionError(f"Access denied")
  
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Cohort file not found")
    
    except URLError as e:
        # Handle file:// protocol errors 
        if "No such file or directory" in str(e):
            raise FileNotFoundError(f"Cohort file not found")
        else:
            raise ConnectionError(f"Unable to connect to location {location}: {str(e)}")
        
    except ValueError:
        # Re-raise ValueError as-is (don't catch in general exception handler)
        raise

    except Exception as e:
        error_msg = f"Unexpected error reading cohort file {location}: {str(e)}"
        raise RuntimeError(error_msg)

def is_cohort_member(nhs_number: str, cohort_members: pd.Series) -> bool:
    """
    Check if the given NHS number belongs to the cohort.

    Args:
        nhs_number (str): The NHS number to check.
        cohort_members (pd.Series): Series of NHS numbers from cohort.

    Returns:
        bool: True if the NHS number is in the cohort, False otherwise.
        
    Raises:
        TypeError: If cohort_members is not a pandas Series.
    """

    logger = logging.getLogger(__name__)
    
    if not nhs_number or str(nhs_number).strip() == "":
        logger.warning(f"NHS number is None or empty, returning False for cohort membership check")
        return False
    
    if not isinstance(cohort_members, pd.Series):
        raise TypeError("cohort_members must be a pandas Series")
        
    if cohort_members.empty:
        return False
    
    try:
        cleaned_cohort_members = cohort_members.astype(str).str.strip().values
        logger.debug(f"NHS number: {nhs_number}. Cohort: {cleaned_cohort_members}")
        
        is_cohort_member = str(nhs_number).strip() in cleaned_cohort_members
        logger.debug(f"NHS number {nhs_number} is member of cohort: {is_cohort_member}")

        return is_cohort_member
    except Exception as e:
        raise RuntimeError(f"Error checking cohort membership for NHS number {nhs_number}: {str(e)}")