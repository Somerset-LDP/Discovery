#!/usr/bin/env bash
set -e

echo "Creating FHIR database..."
if createdb --username "$POSTGRES_USER" hapi; then
    echo "Database 'hapi' created successfully."
else
    echo "Failed to create database 'hapi'."
    exit 1
fi

echo "Creating LDP database..."
if createdb --username "$POSTGRES_USER" ldp; then
    echo "Database 'ldp' created successfully."
else
    echo "Failed to create database 'ldp'."
    exit 1
fi
