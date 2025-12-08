-- Create trace_status table
CREATE TABLE IF NOT EXISTS mpi.trace_status (
    patient_id BIGSERIAL PRIMARY KEY,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);