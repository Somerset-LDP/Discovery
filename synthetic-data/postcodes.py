"""
postcodes.py

Provides utilities for replacing patient postcodes in FHIR bundles using postcode lookup files.
"""

import os
import glob
import random
import json
import re
import logging

log_level_str = os.getenv("LOG_LEVEL", "ERROR").upper()
log_level = getattr(logging, log_level_str, logging.ERROR)
logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')

_postcode_cache = {}
_postcodes_dir = "postcodes"

def replace_patient_postcode(bundle: dict) -> None:    
    current_postcode = bundle['entry'][0]['resource']['address'][0]['postalCode']

    logging.debug(f"Processing patient bundle with current postcode: {current_postcode}")
            
    if current_postcode and isinstance(current_postcode, str):
        new_postcode = _get_random_postcode(current_postcode)
        logging.debug(f"Generated new postcode: {new_postcode}")

        if new_postcode:
            bundle['entry'][0]['resource']['address'][0]['postalCode'] = new_postcode
        else:
            logging.warning(f"Could not find a new postcode for patient with current postcode: {current_postcode}")
   

def _get_random_postcode(outward_code: str) -> str | None:
    postcode = None
    
    match = re.match(r"([A-Za-z]{1,2})\d+", outward_code)
    if match:
        postcode_area = match.group(1)

        postcodes = _get_postcodes_by_area(postcode_area)
        if postcodes:
            postcode = random.choice(postcodes)
        else:
            logging.warning(f"No postcodes found for outward code: {outward_code}")
            
    return postcode


def _get_postcodes_by_area(area: str) -> list:
    postcodes = []

    if area in _postcode_cache:
        postcodes = _postcode_cache[area]
    else:
        # Find matching postcode file
        pattern = os.path.join(_postcodes_dir, f"*{area}.csv")
        matches = glob.glob(pattern)
        if matches:
            postcode_file = matches[0]  
            with open(postcode_file, "r") as f:
                lines = f.readlines()
                # Skip header and get first column from each row
                postcodes = [line.strip().split(",")[0].strip('"') for line in lines[1:] if line.strip()]
            _postcode_cache[area] = postcodes                      
        else:
            logging.warning(f"No postcode file found for outward code area: {area} under the {_postcodes_dir} directory.")

    return postcodes