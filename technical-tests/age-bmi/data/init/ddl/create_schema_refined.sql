    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS "refined";

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO "refined";

    -- Create patient table
    CREATE TABLE IF NOT EXISTS patient (
        patient_id INT PRIMARY KEY,
        dob DATE NOT NULL,
        ethnicity_code VARCHAR(50), -- does this need to be more explicit eg indicate that it's snomed
        ethnicity_code_system VARCHAR(50),
        height_cm NUMERIC(5,2),
        height_observation_time TIMESTAMP,
        weight_kg NUMERIC(5,2),
        weight_observation_time TIMESTAMP,
        sex_code VARCHAR(50),
        sex_code_system VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );