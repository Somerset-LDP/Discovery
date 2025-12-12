-- Location schema DDL for LDP file ingestion tracking
-- Usage: psql -d ldp -f create_schema_location.sql
--
-- Note: After running this script, set the password for location_writer role:
--   ALTER ROLE location_writer WITH PASSWORD 'your-password';
-- Store credentials in AWS Secrets Manager for Lambda access.

-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS "location";

-- Set search_path to the new schema
SET search_path TO "location";

-- Create file ingest log table
CREATE TABLE IF NOT EXISTS ldp_file_ingest_log (
    dataset_key TEXT NOT NULL,
    file_name TEXT NOT NULL,
    checksum TEXT NOT NULL,
    status TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    rows_bronze INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_key, file_name)
);

-- Create read-write user for Lambda access (only if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'location_writer') THEN
        CREATE ROLE location_writer WITH LOGIN;
    END IF;
END
$$;

GRANT CONNECT ON DATABASE ldp TO location_writer;
GRANT USAGE ON SCHEMA "location" TO location_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA "location" TO location_writer;

