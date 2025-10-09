import os
import argparse
from pathlib import Path
import pandas as pd
import json
from jsonschema import validate, ValidationError
from sqlalchemy import Engine, create_engine
import logging
from fhirclient import client
from pipeline_pseudonymised import run_pseudonymised_pipeline
from pipeline_refined import run as run_refined_pipeline
from pipeline_derived import run_derived_pipeline
from typing import Optional

logging.basicConfig(
    filename='logs/ingestion_errors.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Legacy functions removed - pipeline now uses modular approach with 
# dedicated pipeline_pseudonymised.py, pipeline_refined.py, and pipeline_derived.py

def setup_fhir_client(fhir_base_url=None):
    """
    Set up FHIR client for terminology services
    """
    if fhir_base_url is None:
        fhir_base_url = os.getenv("FHIR_BASE_URL", "http://localhost:8080/fhir")
    
    settings = {
        'app_id': 'ldp_pipeline',
        'api_base': fhir_base_url
    }
    
    return client.FHIRClient(settings=settings)

def setup_database_engine(database_url_env_key: str):
    """
    Set up database engines for refined and derived layers
    """
    db_url = os.getenv(database_url_env_key)
    if not db_url:
        raise EnvironmentError(f"Environment variable {database_url_env_key} must be set.")

    engine = create_engine(db_url)

    return engine

def run(input_file_path: Path, pseudonymised_store: Optional[Path] = None, refined_store: Optional[Engine] = None, derived_store: Optional[Engine] = None, fhir_client: Optional[client.FHIRClient] = None):
    """
    Orchestrate the complete data pipeline from raw input to derived analytics
    
    Args:
        input_file_path: Path to raw input JSON file
        
    Returns:
        dict: Summary of pipeline execution with storage locations
    """
    from datetime import datetime
    
    # Get configuration from environment variables
    if not pseudonymised_store:
        pseudonymised_store = Path(os.getenv("PSEUDONYMISED_STORE_PATH", "pseudonymised"))
    
    # Set up infrastructure
    if not fhir_client:
        fhir_client = setup_fhir_client()

    if not refined_store:
        refined_store = setup_database_engine("REFINED_DATABASE_URL")

    if not derived_store:
        derived_store = setup_database_engine("DERIVED_DATABASE_URL")
    
    print(f"Starting pipeline execution...")
    print(f"Input: {input_file_path}")
    print(f"Pseudonymised store: {pseudonymised_store}")
    
    # Stage 1: Pseudonymised Pipeline
    print("\n=== Stage 1: Pseudonymised Pipeline ===")
    pseudonymised_store_latest = run_pseudonymised_pipeline(
        input_path=input_file_path,
        pseudonymised_store=pseudonymised_store
    )
    print(f"Pseudonymised data written to:")
    print(f"  Raw: {pseudonymised_store_latest}/raw/patients.json")
    print(f"  Calculated: {pseudonymised_store_latest}/calculated/patients.json")
    
    # Stage 2: Refined Pipeline  
    print("\n=== Stage 2: Refined Pipeline ===")
    # Use the directory containing both raw and calculated data
    refined_store = run_refined_pipeline(
        pseudonymised_store=str(pseudonymised_store_latest),
        refined_store=refined_store,
        fhir_client=fhir_client
    )
    print(f"Refined data written to database: {refined_store.url}")
    
    # Stage 3: Derived Pipeline
    print("\n=== Stage 3: Derived Pipeline ===") 
    # Use current time as changed_since to process all data
    changed_since = datetime.min  # Process all data
    run_derived_pipeline(
        changed_since=changed_since,
        refined_store=refined_store,
        derived_store=derived_store,
        fhir_client=fhir_client
    )
    print(f"Derived data written to database: {derived_store.url}")

    print("\n=== Pipeline Complete ===")
    
    return {
        "pseudonymised": {
            "raw": f"{pseudonymised_store_latest}/raw/patients.json",
            "calculated": f"{pseudonymised_store_latest}/calculated/patients.json"
        },
        "refined_engine_url": str(refined_store.url),
        "derived_engine_url": str(derived_store.url),
        "execution_time": datetime.now().isoformat()
    }

def main():
    """
    Command line entry point
    """
    parser = argparse.ArgumentParser(
        description="Run the complete demonstrator data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python pipeline.py data/raw_stored.json
        """
    )
    
    parser.add_argument(
        "input_file", 
        help="Path to input JSON file containing raw patient data"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file not found: {args.input_file}")
        return 1
    
    try:
        # Run the pipeline with internally determined settings
        result = run(input_file_path=args.input_file)
        
        print(f"\nPipeline execution summary:")
        print(f"Execution time: {result['execution_time']}")
        print(f"Pseudonymised raw: {result['pseudonymised']['raw']}")
        print(f"Pseudonymised calculated: {result['pseudonymised']['calculated']}")
        print(f"Refined database: {result['refined_engine_url']}")
        print(f"Derived database: {result['derived_engine_url']}")
        
        return 0
        
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
