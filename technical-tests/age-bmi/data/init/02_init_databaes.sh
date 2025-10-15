#!/usr/bin/env bash
set -e

# Ensure we are in the init directory (changes to the directory where the script itself resides)
cd "$(dirname "$0")"

# --- Configuration variables ---
DB_NAME="ldp"

# --- Create refined schema and tables/views ---
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" -f "$(pwd)/ddl/create_schema_refined.sql"
then
    echo "Tables and Views created successfully in schema 'refined'."
else
    echo "Failed to create Tables and Views in schema 'refined'."
    exit 1
fi

# --- Create Derived schema and tables/views ---
if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DB_NAME" -f "$(pwd)/ddl/create_schema_derived.sql"
then
    echo "Tables and Views created successfully in schema 'derived'."
else
    echo "Failed to create Tables and Views in schema 'derived'."
    exit 1
fi