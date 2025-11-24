#!/usr/bin/env bash
set -e

# Ensure we are in the init directory (changes to the directory where the script itself resides)
cd "$(dirname "$0")"

# Check if the database exists
DB_EXISTS=$(psql -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='ldp'")

if [ "$DB_EXISTS" != "1" ]; then
  echo "Creating LDP database..."
  #psql -U "$POSTGRES_USER" --dbname postgres -c "CREATE DATABASE ldp;"
  createdb -U "$POSTGRES_USER" ldp
else
  echo "Database 'ldp' already exists."
fi