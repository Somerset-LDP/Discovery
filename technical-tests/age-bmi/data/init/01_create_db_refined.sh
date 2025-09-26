#!/usr/bin/env bash
set -e

echo "Creating refined database..."
if createdb --username "$POSTGRES_USER" refined; then
    echo "Database 'refined' created successfully."
else
    echo "Failed to create database 'refined'."
    exit 1
fi
