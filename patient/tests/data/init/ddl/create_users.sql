-- usage - psql -d your_database -v user_password="'MySecurePassword123!'" -f create_schema_mpi.sql

\if :{?user_password}
    -- user_password is already set, do nothing
\else
    \set user_password 'DefaultPassword123!'
\endif

-- Create schema if it does not exist
CREATE SCHEMA IF NOT EXISTS "mpi";

-- Create read-write (no delete) user with parameterized password
-- First drop the user if it exists to avoid conflicts
DROP ROLE IF EXISTS mpi_writer;
CREATE ROLE mpi_writer WITH LOGIN PASSWORD :'user_password';

GRANT CONNECT ON DATABASE ldp TO mpi_writer;

GRANT USAGE ON SCHEMA "mpi" TO mpi_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA "mpi" TO mpi_writer;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA "mpi" TO mpi_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA "mpi" GRANT SELECT, INSERT, UPDATE ON TABLES TO mpi_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA "mpi" GRANT SELECT, USAGE ON SEQUENCES TO mpi_writer;