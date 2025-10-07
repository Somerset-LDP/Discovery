    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS "derived";

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO "derived";

    CREATE TABLE IF NOT EXISTS patient (
        patient_id INT PRIMARY KEY,
        bmi NUMERIC(5,2),
        bmi_calculation_time TIMESTAMP,
        bmi_category VARCHAR(50),
        bmi_category_system VARCHAR(50), 
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );