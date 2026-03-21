"""
generate.py

Main entry point for generating synthetic patient data using Synthea and post-processing the output.
This script runs the Synthea then applies postcode replacements to the generated FHIR bundles using 
utilities from postcodes.py.

Environment Variables:
    LOG_LEVEL: Set the logging level (default: ERROR).

Usage:
    python generate.py --region "South West" --populationSize 10 --city "Dorchester"
"""

import argparse
from datetime import datetime
import os
import random
import subprocess
import sys
import logging
import time
import csv
import json
import uuid
from postcodes import replace_patient_postcode

log_level_str = os.getenv("LOG_LEVEL", "ERROR").upper()
log_level = getattr(logging, log_level_str, logging.ERROR)
logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s: %(message)s')

def _main(populationSize: int, region: str, city: str, serialisers: list) -> None:
    logging.info(f"Starting Synthea simulation with population size: {populationSize}, region: {region}, city: {city}, serialisers: {serialisers}")
    
    start = time.perf_counter()
    
    output_dir = _run_synthea(populationSize, region, city)

    _post_process_fhir_bundles(output_dir)

    # start serialisation
    if serialisers:
        _serialise(_get_patient_files(output_dir), output_dir, serialisers)

    end = time.perf_counter()
    
    logging.info(f"Synthea simulation completed in {end - start:.2f} seconds.")

def _run_synthea(populationSize: int, region: str, city: str) -> str:
    output_dir = os.path.join("output", "fhir")

    cmd = [
        "java",
        "-jar", "synthea-with-dependencies.jar",
        "-p", str(populationSize),
        region
    ]

    if city:
        cmd.append(city)

    cmdline = " ".join(f'"{a}"' if " " in a else a for a in cmd)
    logging.debug(f"Running Synthea with command: {cmdline}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.debug(f"Synthea output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running Synthea:\n{e.stderr}")
        sys.exit(1) 

    return output_dir   

def _post_process_fhir_bundles(output_dir: str) -> None:
    logging.debug(f"Starting post-processing of FHIR bundles in directory {os.path.join(os.getcwd(), output_dir)}.")
    # list the output dir and filter for patient files
    patient_files = _get_patient_files(output_dir)

    for patient_file in patient_files:
       
        with open(patient_file, "r", encoding="utf-8") as f:   
            bundle = json.load(f)    

            replace_patient_postcode(bundle)
            _add_condition_recorder(bundle)
            _add_encounter_hospitalization(bundle)

        with open(patient_file, "w", encoding="utf-8") as f:
            json.dump(bundle, f, indent=2)

    logging.debug("Completed post-processing of FHIR bundles.")

def _add_condition_recorder(bundle: dict) -> None:
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Condition":
            recorder = resource.setdefault("recorder", {})
            recorder["reference"] = f"Practitioner/{uuid.uuid4().hex[:8]}_does_not_exist" 

def _add_encounter_hospitalization(bundle: dict) -> None:
    encounters = []

    for entry in bundle.get("entry", []):
        
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Encounter":
            hospitalization = resource.setdefault("hospitalization", {})
            hospitalization["admitSource"] =  {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/admit-source",
                    "code": random.choice(["hosp-trans", "emd", "outp", "born", "gp", "mp", "nursing", "psych", "rehab", "other"])
                }]
            }

            encounters.append(resource)

    # grab the last encounter and randomly assign discharge disposition
    last_encounter = encounters[-1]
    hospitalization = last_encounter.setdefault("hospitalization", {})
    hospitalization["dischargeDisposition"] =  {
        "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/discharge-disposition",
            "code": random.choice(["hosp", "home", "alt-home", "other-hcf", "long", "aadvice", "psy", "exp", "rehab", "snf", "oth"])
        }]
    }            

def _get_patient_files(patients_dir: str) -> list:
    return [
        os.path.join(patients_dir, entry)
        for entry in os.listdir(patients_dir)
        if os.path.isfile(os.path.join(patients_dir, entry)) and not entry.startswith(("hospital", "practitioner"))
    ]

#################################################################################
#                                                                               #
#                           SERIALISER IMPLEMENTATIONS                          #
#                                                                               #
#  Add new serialiser functions below this block. Each serialiser should take   #
#  FHIR bundle files and output transformed data in the required format into a  #
#  given output directory.                                                      #
#                                                                               #
#  Example:                                                                     #
#      def _to_<your_serialiser>(fhir_bundle_files: list, output_dir: str):     #
#          ...                                                                  #
#                                                                               #
#  Existing serialisers:                                                        #
#      - _to_sft                                                                #
#                                                                               #
#################################################################################

def _serialise(fhir_bundle_files: list, output_dir: str, serialisers: list) -> None:
    serialiser_funcs = {
        "sft": _to_sft,
        "emis": _to_emis,     
        "council": _to_council
    }

    for serialiser in serialisers:
        func = serialiser_funcs.get(serialiser)
        if func:
            func(fhir_bundle_files, output_dir)
        else:
            logging.warning(f"Serialiser '{serialiser}' not implemented.")    

# Somerset Foundation Trust (SFT) serialiser

def _get_identifier_by_code(resource: dict, code: str) -> str | None:
    for identifier in resource.get("identifier", []):
        type_ = identifier.get("type", {})
        for coding in type_.get("coding", []):
            if coding.get("code") == code:
                return identifier.get("value")
    return None  

def _to_council(fhir_bundle_files: list, output_dir: str) -> None:
    council_output_dir = os.path.join(output_dir, "council")
    os.makedirs(council_output_dir, exist_ok=True)
    logging.debug(f"Starting Council serialisation for files: {fhir_bundle_files} with output directory: {council_output_dir}")

    # TODO - implement me    

    logging.debug(f"Completed Council serialisation. Generated files: {os.listdir(council_output_dir)}")

def _to_emis(fhir_bundle_files: list, output_dir: str) -> None:  
    emis_output_dir = os.path.join(output_dir, "emis")
    os.makedirs(emis_output_dir, exist_ok=True)
    logging.debug(f"Starting EMIS serialisation for files: {fhir_bundle_files} with output directory: {emis_output_dir}")   

    # TODO - implement me

    logging.debug(f"Completed EMIS serialisation. Generated files: {os.listdir(emis_output_dir)}") 

def _to_sft(fhir_bundle_files: list, output_dir: str) -> None:
    sft_output_dir = os.path.join(output_dir, "sft")
    os.makedirs(sft_output_dir, exist_ok=True)
    logging.debug(f"Starting SFT serialisation for files: {fhir_bundle_files} with output directory: {sft_output_dir}")

    patients = []
    conditions = []
    encounters = []

    for file_path in fhir_bundle_files:
        with open(file_path) as f:
            bundle = json.load(f)
        for entry in bundle.get("entry", []):
            pas_number = None
            
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Patient":

                pas_number = _get_identifier_by_code(resource, "MR") # medical record number
                nhs_number = _get_identifier_by_code(resource, "SS") # use US Social Security number as a stand-in for NHS number

                patients.append({
                    "first_name": resource.get("name", [{}])[0].get("given", [""])[0],
                    "last_name": resource.get("name", [{}])[0].get("family", ""),
                    "date_of_birth": resource.get("birthDate", ""),
                    "sex": resource.get("gender", ""),
                    "postcode": resource.get("address", [{}])[0].get("postalCode", ""),
                    "first_line_of_address": resource.get("address", [{}])[0].get("line", [""])[0],
                    "pas_number": pas_number,
                    "nhs_number": nhs_number
                })
            elif resource.get("resourceType") == "Condition":
                conditions.append({
                    "condition_occurrence_id": resource.get("id", ""),
                    "spell_id": resource.get("encounter", {}).get("reference", ""),
                    "start_date": resource.get("onsetDateTime", "")[:10],
                    "end_date": resource.get("abatementDateTime", "")[:10],
                    "condition_code": resource.get("code", {}).get("code", ""),
                    "pas_number": pas_number,
                    "provider_id": resource.get("recorder", {}).get("reference", ""),
                    "visit_id": f"visit_random_{uuid.uuid4().hex[:8]}_does_not_exist",
                    "condition_stop_reason" : random.choice(["superseded", "entered-in-error", "administrative", ""]) # SFT specific concept. Not in FHIR model
                })
            elif resource.get("resourceType") == "Encounter":
                encounters.append({
                    "spell_id": resource.get("id", ""),
                    "type": resource.get("type", [{}])[0].get("coding", [{}])[0].get("code", ""),
                    "start_date": resource.get("period", {}).get("start", "")[:10],
                    "end_date": resource.get("period", {}).get("end", "")[:10],
                    "provider_id": resource.get("participant", [{}])[0].get("individual", {}).get("reference", ""),
                    "pas_number": pas_number,
                    "admitted_from": resource.get("hospitalization", {}).get("admitSource", {}).get("coding", [{}])[0].get("code", ""),
                    "discharged_to": resource.get("hospitalization", {}).get("dischargeDisposition", {}).get("coding", [{}])[0].get("code", "")
                })

    # Write CSVs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    with open(os.path.join(sft_output_dir, f"patients_{timestamp}.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "first_name", "last_name", "date_of_birth", "sex", "postcode", "first_line_of_address", "pas_number", "nhs_number"
        ])
        writer.writeheader()
        writer.writerows(patients)

    with open(os.path.join(sft_output_dir, f"conditions_{timestamp}.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "condition_occurrence_id", "spell_id", "start_date", "end_date", "condition_code", "pas_number", "provider_id", "visit_id", "condition_stop_reason"
        ])
        writer.writeheader()
        writer.writerows(conditions)

    with open(os.path.join(sft_output_dir, f"encounters_{timestamp}.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "spell_id", "type", "start_date", "end_date", "provider_id", "pas_number", "admitted_from", "discharged_to"
        ])
        writer.writeheader()
        writer.writerows(encounters)

    logging.debug(f"Completed SFT serialisation. Generated files: {os.listdir(sft_output_dir)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Synthea simulation.")
    parser.add_argument("--region", default="South West", help="Region to simulate (default: South West)")
    parser.add_argument("--city", help="City to simulate (optional)")
    parser.add_argument("--populationSize", type=int, default=1, help="Population size (default: 1)")
    parser.add_argument(
        "--serialisers",
        nargs="+",
        choices=["sft", "emis", "council"],
        required=False,
        help="Serialisers to run (choose one or more: sft, emis, council)"
    )    

    args = parser.parse_args()

    _main(args.populationSize, args.region, args.city, args.serialisers)