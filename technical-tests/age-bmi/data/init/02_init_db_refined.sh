#!/usr/bin/env bash
set -e

if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname refined <<-EOSQL
    -- Create age bucket table
    CREATE TABLE IF NOT EXISTS age_range (
        id INT PRIMARY KEY,
        use_case VARCHAR(50),
        min_age INT,
        max_age INT,
        label VARCHAR(20)
    );

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
    echo "Tables and Views created successfully."
else
    echo "Failed to create Tables and Views"
    exit 1
fi
