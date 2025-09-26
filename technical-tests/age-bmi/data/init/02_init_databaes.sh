#!/usr/bin/env bash
set -e

# --- Configuration variables ---
DB_NAME="ldp"

# --- Create refined schema and tables/views ---
SCHEMA_NAME="refined"
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" <<-EOSQL
    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME;

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO $SCHEMA_NAME;

    -- Create age bucket table
    CREATE TABLE IF NOT EXISTS age_range (
        id INT PRIMARY KEY,
        use_case VARCHAR(50),
        min_age INT,
        max_age INT,
        label VARCHAR(20)
    );

    -- Create patient table
    CREATE TABLE IF NOT EXISTS patient (
        patient_id INT PRIMARY KEY,
        dob DATE NOT NULL,
        height_cm NUMERIC(5,2),
        height_observation_time TIMESTAMP,
        weight_kg NUMERIC(5,2),
        weight_observation_time TIMESTAMP,
        bmi NUMERIC(5,2),
        bmi_calculation_time TIMESTAMP
    );

    -- Create materialized view: patient count by age range for population health
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_population_health_age_distribution AS
    SELECT 
        ar.label AS age_range,
        COUNT(p.patient_id) AS patient_count
    FROM patient p
    JOIN age_range ar
      ON (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM p.dob))
         BETWEEN ar.min_age AND ar.max_age
    WHERE ar.use_case = 'population_health'
    GROUP BY ar.label
    ORDER BY MIN(ar.min_age);
EOSQL
then
    echo "Tables and Views created successfully in schema '$SCHEMA_NAME'."
else
    echo "Failed to create Tables and Views in schema '$SCHEMA_NAME'."
    exit 1
fi

# --- Create Derived schema and tables/views ---
SCHEMA_NAME="derived"
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" <<-EOSQL
    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME;

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO $SCHEMA_NAME;

    CREATE TABLE patient (
        patient_id INT PRIMARY KEY,
        bmi           NUMERIC(5,2),
        bmi_calculation_time TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS patient_weight_category (
        patient_id INT NOT NULL,
        scheme_name VARCHAR(50) NOT NULL,
        category VARCHAR(20) NOT NULL,
        calculation_time TIMESTAMP NOT NULL,
        PRIMARY KEY (patient_id, scheme_name)
    );    

    CREATE TABLE report_definition (
        report_id      BIGSERIAL PRIMARY KEY,
        name           VARCHAR(100) NOT NULL,
        description    TEXT,
        metrics        JSONB,       -- e.g., {"bmi": "AVG", "weight": "MAX"}
        dimensions     JSONB,       -- e.g., ["age_group", "gender"]
        filters        JSONB,       -- e.g., {"bmi_category": "Obese"}
        created_at     TIMESTAMP DEFAULT NOW(),
        updated_at     TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE report_cache (
        report_id    BIGINT REFERENCES report_definition(report_id),
        run_time     TIMESTAMP NOT NULL,
        result       JSONB NOT NULL
    );


    CREATE INDEX idx_bmi_category ON patient_bmi(bmi_category);
EOSQL
then
    echo "Tables and Views created successfully in schema '$SCHEMA_NAME'."
else
    echo "Failed to create Tables and Views in schema '$SCHEMA_NAME'."
    exit 1
fi

