import os
import psycopg2
from pathlib import Path
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432"))
    )

def run_create_tables():
    # Resolve the path relative to this file
    sql_path = Path(__file__).parent / "data" / "creation" / "01_create_tables.sql"
    
    # Make sure the file exists
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    with open(sql_path, "r") as f:
        sql_script = f.read()

    conn = get_connection() 
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit()
    cur.close()
    conn.close()

def store_refined(records):
    connection = get_connection()
    cur = connection.cursor()

    for r in records:
        obs_time = datetime.fromisoformat(r['observation_time'])

        # Insert patient
        cur.execute("""
            INSERT INTO patient (patient_id, dob)
            VALUES (%s, %s)
            ON CONFLICT (patient_id) DO NOTHING
        """, (r['patient_id'], r['dob']))

        # Height
        if r.get('height_cm') is not None:
            cur.execute("""
                INSERT INTO observations (
                    patient_id, observation_type_code, observation_type_system,
                    value, unit_code, unit_system, observation_time,
                    processed_at, source_file, source_value
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
            """, (r['patient_id'], 'HEIGHT', 'http://loinc.org',
                  r['height_cm'], 'cm', 'http://unitsofmeasure.org',
                  obs_time, r.get('source_file'), r.get('height')))

        # Weight
        if r.get('weight_kg') is not None:
            cur.execute("""
                INSERT INTO observations (
                    patient_id, observation_type_code, observation_type_system,
                    value, unit_code, unit_system, observation_time,
                    processed_at, source_file, source_value
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
            """, (r['patient_id'], 'WEIGHT', 'http://loinc.org',
                  r['weight_kg'], 'kg', 'http://unitsofmeasure.org',
                  obs_time, r.get('source_file'), r.get('weight')))

        # BMI
        if r.get('bmi') is not None:
            cur.execute("""
                INSERT INTO observations (
                    patient_id, observation_type_code, observation_type_system,
                    value, unit_code, unit_system, observation_time,
                    processed_at, source_file, source_value
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s)
            """, (r['patient_id'], 'BMI', 'http://loinc.org',
                  r['bmi'], 'kg/m2', 'http://unitsofmeasure.org',
                  obs_time, r.get('source_file'), str(r['bmi'])))
    connection.commit()
    cur.close()
    connection.close()    
