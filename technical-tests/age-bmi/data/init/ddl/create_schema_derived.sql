    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS "derived";

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO "derived";

    CREATE TABLE IF NOT EXISTS patient (
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

