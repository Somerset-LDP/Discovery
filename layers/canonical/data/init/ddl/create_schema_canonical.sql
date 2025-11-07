-- usage - psql -d your_database -v user_password="'MySecurePassword123!'" -f create_schema_canonical.sql

\if :{?user_password}
    -- user_password is already set, do nothing
\else
    \set user_password 'DefaultPassword123!'
\endif

    -- Create schema if it does not exist
    CREATE SCHEMA IF NOT EXISTS "canonical";

    -- Set search_path to the new schema so tables/views are created there
    SET search_path TO "canonical";

    -- Create patient table
    CREATE TABLE IF NOT EXISTS patient (
        nhs_number TEXT NOT NULL, 
        given_name TEXT NOT NULL,
        family_name TEXT NOT NULL,
        date_of_birth TEXT NOT NULL,
        postcode TEXT NOT NULL,
        sex TEXT,
        height_cm  NUMERIC(5,2),
        height_observation_time TIMESTAMP,
        weight_kg  NUMERIC(5,2),
        weight_observation_time TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create read-write (no delete) user with parameterized password
-- First drop the user if it exists to avoid conflicts
DROP ROLE IF EXISTS canonical_writer;
CREATE ROLE canonical_writer WITH LOGIN PASSWORD :'user_password';

GRANT CONNECT ON DATABASE ldp TO canonical_writer;

GRANT USAGE ON SCHEMA "canonical" TO canonical_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA "canonical" TO canonical_writer;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA "canonical" TO canonical_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA "canonical" GRANT SELECT, INSERT, UPDATE ON TABLES TO canonical_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA "canonical" GRANT SELECT, USAGE ON SEQUENCES TO canonical_writer;