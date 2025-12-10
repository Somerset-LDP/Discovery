-- Create patient table
CREATE TABLE IF NOT EXISTS mpi.patient (
    patient_id BIGSERIAL PRIMARY KEY,
    nhs_number TEXT, 
    given_name TEXT,
    family_name TEXT,
    date_of_birth TEXT,
    postcode TEXT,
    sex TEXT,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);