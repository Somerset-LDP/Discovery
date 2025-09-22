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
    dob DATE
);

-- Create Observations linked to patients
CREATE TABLE IF NOT EXISTS observations (
    observation_id SERIAL PRIMARY KEY,
    patient_id INT NOT NULL REFERENCES patient(patient_id),
    observation_type_code VARCHAR(50) NOT NULL,  -- e.g., 'BMI', 'HEIGHT', 'WEIGHT'
    observation_type_system VARCHAR(255) NOT NULL, -- e.g., 'http://loinc.org'
    value NUMERIC NOT NULL,
    unit_code VARCHAR(50) NOT NULL,             -- e.g., 'kg', 'cm'
    unit_system VARCHAR(255) NOT NULL,          -- e.g., 'http://unitsofmeasure.org'
    observation_time TIMESTAMP WITH TIME ZONE NOT NULL, -- time measurement was taken
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT now(), -- time inserted into DB    
    source_file VARCHAR(255),                   -- trace back to raw file
    source_value VARCHAR(50)                   -- original value before conversion
);

