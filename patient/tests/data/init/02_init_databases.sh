#!/usr/bin/env bash
set -e

# Ensure we are in the init directory (changes to the directory where the script itself resides)
cd "$(dirname "$0")"

# --- Configuration variables ---
DB_NAME="ldp"

# --- Create mpi schema and tables/views ---
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" -f "$(pwd)/ddl/create_users.sql"
then
    echo "Tables and Views created successfully in schema 'mpi'."
else
    echo "Failed to create Tables and Views in schema 'mpi'."
    exit 1
fi