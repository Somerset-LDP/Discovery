#!/usr/bin/env python3
"""
Demo script for the Age-BMI Data Pipeline

This script demonstrates the complete data pipeline execution from raw patient data
through pseudonymised, refined, and derived layers. It validates the environment,
sets up temporary storage, and orchestrates the full pipeline.

Prerequisites:
- Docker services running (docker-compose up -d)
- Required environment variables set
- Python dependencies installed

Usage:
    python demo.py <input_file>
    
Example:
    python demo.py demo/raw_patients.json
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from tabulate import tabulate

from pipeline import run as run_pipeline


def setup_logging():
    """Configure logging for the entire application"""
    # Create temporary directory for logs
    log_dir = Path(tempfile.mkdtemp(prefix='age_bmi_logs_'))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'pipeline.log'),
            logging.StreamHandler()  # Also show logs in console
        ]
    )
    
    print(f"Logs will be written to: {log_dir / 'pipeline.log'}")


def setup_environment():
    """
    Check that all required environment variables are set
    
    Returns:
        bool: True if all required vars are set, False otherwise
    """
    # Load environment variables from .env file
    try:
        load_dotenv()  # Automatically looks for .env in current directory and parent directories
        print("Loaded environment variables from .env file")
    except ImportError:
        print(".env file not found, environment variables must be set manually")

    required_vars = {
        'FHIR_BASE_URL',
        'REFINED_DATABASE_URL', 
        'DERIVED_DATABASE_URL',
        'SNOMED_BODY_HEIGHT',
        'SNOMED_BODY_WEIGHT'
    }
        
    missing_vars = []
    
    for var_name in required_vars:
        value = os.getenv(var_name)
        if not value:
            missing_vars.append(var_name)
   
    if missing_vars:
        print(f"\nMissing required environment variables: {', '.join(missing_vars)}")
        print("\nAll environment variables are mandatory. Please set them in your .env file or environment.")
        return False
    
    print("\nAll environment variables are properly configured")
    return True

def create_pseudonymised_store():
    """
    Create a temporary directory for the pseudonymised store
    
    Returns:
        Path: Path to the temporary directory
    """
    print("\n=== Creating Temporary Storage ===")
    
    temp_dir = Path(tempfile.mkdtemp(prefix='ldp_demo_', suffix='_pseudonymised'))
    print(f"Created temporary pseudonymised store: {temp_dir}")
    
    # Set environment variable for the pipeline
    os.environ['PSEUDONYMISED_STORE_PATH'] = str(temp_dir)
    #print(f"Set PSEUDONYMISED_STORE_PATH={temp_dir}")
    
    return temp_dir

def load_and_display_json(file_path: Path, title: str):
    """Load JSON file and display formatted summary"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            print(f"{title}: {len(data)} records")
        else:
            print(f"{title}: Single record")
            
    except Exception as e:
        print(f"{title}: Error loading file - {e}")


def get_table_count(engine, table_name: str) -> int:
    """Get record count from database table"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()
    except Exception as e:
        print(f"Error counting records in {table_name}: {e}")
        return 0


def display_table(engine, table_name: str, limit: int = 5):
    """Execute SQL query and display formatted results"""
    try:
        df = pd.read_sql(text(f"SELECT * FROM {table_name} LIMIT {limit}"), engine)
        
        if len(df) > 0:
            print(f"\nData data from {table_name}:")
            print(tabulate(df.values.tolist(), headers=df.columns.tolist(), tablefmt='grid', showindex=False))
        else:
            print(f"No data found in {table_name}")
            
    except Exception as e:
        print(f"Error displaying {table_name}: {e}")

def display_pseudonymised_data(temp_store: Path):
    """Show contents of temporary pseudonymised JSON files"""
    print(f"\n--- Pseudonymised Storage (JSON Files) ---")
    
    # Find the date-structured directory (e.g., 2025/10/09/)
    date_dirs = []
    for year_dir in temp_store.iterdir():
        if year_dir.is_dir() and year_dir.name.isdigit():
            for month_dir in year_dir.iterdir():
                if month_dir.is_dir() and month_dir.name.isdigit():
                    for day_dir in month_dir.iterdir():
                        if day_dir.is_dir() and day_dir.name.isdigit():
                            date_dirs.append(day_dir)
    
    if not date_dirs:
        print("No date-structured directories found in pseudonymised store")
        return
        
    # Use the first (most recent) date directory
    date_dir = sorted(date_dirs)[-1]
    
    # Raw data
    raw_file = date_dir / "raw" / "patients.json"
    if raw_file.exists():
        load_and_display_json(raw_file, "Raw Patients")
    else:
        print("Raw patients file not found")


def display_refined_data(db_url: str):
    """Show contents of refined database tables"""
    print(f"\n--- Refined Storage (PostgreSQL) ---")
    
    try:
        engine = create_engine(db_url)
        
        # Patient count
        patient_count = get_table_count(engine, "refined.patient")
        print(f"Refined Patients: {patient_count} records")
        
        # Sample data (first 5 records)
        if patient_count > 0:
            display_table(engine, "refined.patient", limit=5)
            
    except Exception as e:
        print(f"Error accessing refined database: {e}")


def display_derived_data(db_url: str):
    """Show contents of derived database tables"""
    print(f"\n--- Derived Storage (PostgreSQL) ---")
    
    try:
        engine = create_engine(db_url)
        
        # Patient count with BMI calculations
        patient_count = get_table_count(engine, "derived.patient")
        print(f"Derived Patients: {patient_count} records")
        
        # BMI statistics and sample data
        if patient_count > 0:
            display_table(engine, "derived.patient", limit=5)
            
    except Exception as e:
        print(f"Error accessing derived database: {e}")


def display_pipeline_data():
    """
    Display data from all three pipeline storage layers
    """
    print(f"\n=== Pipeline Data Contents ===")

    pseudonymised_store_path = os.environ.get('PSEUDONYMISED_STORE_PATH')
    refined_store_url = os.getenv('REFINED_DATABASE_URL')
    derived_store_url = os.getenv('DERIVED_DATABASE_URL')
    if pseudonymised_store_path and refined_store_url and derived_store_url:    
        # Show pseudonymised data
        display_pseudonymised_data(Path(pseudonymised_store_path))

        # Show refined data
        display_refined_data(refined_store_url)

        # Show derived data
        display_derived_data(derived_store_url)
    else:
        print("Cannot display pipeline data: Missing environment variables for storage locations")


def main():
    """
    Main demo execution function
    """
    parser = argparse.ArgumentParser(
        description="Demo the Age-BMI Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python demo.py test/raw_patients.json
    
Prerequisites:
    1. Start Docker services: docker-compose up -d
    2. Set environment variables in .env file
    3. Install Python dependencies: pip install -r requirements.txt
        """
    )
    
    parser.add_argument(
        "input_file",
        help="Path to input JSON file containing raw patient data"
    )
    
    args = parser.parse_args()
    
    print("Age-BMI Data Pipeline Demo")
    print("=" * 50)
    
    input_file_path = Path(args.input_file)
    
    try:
        # Step 0: Setup logging
        setup_logging()
        
        # Step 1: Validate environment
        if not setup_environment():
            return 1
        
        # Step 2: Validate input file
        if not input_file_path.exists() or not input_file_path.is_file():
            print(f"Input file not found or is a directory: {input_file_path}")
            return 1
        
        # Step 3: Create temporary storage
        create_pseudonymised_store()
        #pseudonymised_store_path = Path("/tmp/ldp_demo_5_6qp1zg_pseudonymised/")

        # Step 4: Run the pipeline
        run_pipeline(input_file_path=input_file_path)
        print(f"\nPipeline completed successfully!")

        # Step 5: Display pipeline data contents
        display_pipeline_data()

        print(f"\nDemo completed successfully!")
        
        return 0
        
    except KeyboardInterrupt:
        print(f"\n\nDemo interrupted by user")
        return 130
        
    except Exception as e:
        print(f"\nDemo failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())