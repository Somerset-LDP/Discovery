-- Create patient table
CREATE TABLE IF NOT EXISTS mpi.patient (
    patient_id BIGSERIAL PRIMARY KEY,
    nhs_number TEXT NOT NULL, 
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    date_of_birth TEXT NOT NULL,
    postcode TEXT NOT NULL,
    sex TEXT,
    verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);