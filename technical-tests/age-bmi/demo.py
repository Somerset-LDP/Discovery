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

def create_temp_pseudonymised_store():
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
    print(f"Set PSEUDONYMISED_STORE_PATH={temp_dir}")
    
    return temp_dir


def validate_input_file(input_file_path: Path):
    """
    Validate that the input file exists and is readable
    
    Args:
        input_file_path: Path to the input file
        
    Returns:
        bool: True if file is valid, False otherwise
    """
    print(f"\n=== Input File Validation ===")
    
    if not input_file_path.exists():
        print(f"Input file not found: {input_file_path}")
        return False
    
    if not input_file_path.is_file():
        print(f"Input path is not a file: {input_file_path}")
        return False
    
    # Try to read and parse JSON
    try:
        with open(input_file_path, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"Input file must contain a JSON array of patients")
            return False

        print(f"Input file is valid JSON with {len(data)} patient records")
        return True
        
    except json.JSONDecodeError as e:
        print(f"Input file contains invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"Error reading input file: {e}")
        return False


def run_demo_pipeline(input_file_path: Path, temp_store: Path):
    """
    Execute the complete pipeline with the given input
    
    Args:
        input_file_path: Path to the input JSON file
        temp_store: Path to temporary pseudonymised store
        
    Returns:
        dict: Pipeline execution results
    """
    print(f"\n=== Pipeline Execution ===")
    print(f"Input file: {input_file_path}")
    print(f"Temporary store: {temp_store}")
    
    try:
        # Run the complete pipeline
        result = run_pipeline(input_file_path=input_file_path)
        
        print(f"\nPipeline completed successfully!")
        return result
        
    except Exception as e:
        print(f"\nPipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        raise

def print_pipeline_summary(result: dict):
    """
    Print a summary of the pipeline execution results
    
    Args:
        result: Pipeline execution result dictionary
    """
    print(f"\n=== Pipeline Summary ===")
    print(f"Execution time: {result['execution_time']}")
    print(f"\nData Locations:")
    print(f"  Pseudonymised (raw): {result['pseudonymised']['raw']}")
    print(f"  Pseudonymised (calculated): {result['pseudonymised']['calculated']}")
    print(f"  Refined database: {result['refined_engine_url']}")
    print(f"  Derived database: {result['derived_engine_url']}")


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
    temp_store = None
    
    try:
        # Step 0: Setup logging
        setup_logging()
        
        # Step 1: Validate environment
        if not setup_environment():
            return 1
        
        # Step 2: Validate input file
        if not validate_input_file(input_file_path):
            return 1
        
        # Step 3: Create temporary storage
        temp_store = create_temp_pseudonymised_store()
        
        # Step 4: Run the pipeline
        result = run_demo_pipeline(input_file_path, temp_store)
        
        # Step 5: Print summary
        print_pipeline_summary(result)
        
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