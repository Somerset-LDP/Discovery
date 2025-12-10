# Checksum Lambda

Lambda function triggered by S3 events that processes reference data files arriving in the Landing zone.

> **Note**: The original HLD specified DynamoDB for storing ingestion records. However, due to the use of RDS for
> matching and FHIR purposes, the decision was made to store this data in RDS as well.

## Overview

This function:

1. Calculates SHA256 checksum for files arriving in Landing bucket
2. Checks RDS database for existing records
3. Copies file to Bronze bucket if new/changed
4. Updates database record with status `bronze_done`
5. Removes file from Landing bucket

## Trigger

- **Event**: S3 ObjectCreated:*
- **Bucket**: `ldp-zone-a-landing`
- **Prefix**: `landing/reference/`

## Environment Variables

| Variable                 | Description                                                  |
|--------------------------|--------------------------------------------------------------|
| `BRONZE_BUCKET`          | Destination S3 bucket name (required)                        |
| `LDP_DB_HOST`            | PostgreSQL database host (required)                          |
| `LDP_DB_PORT`            | PostgreSQL database port (default: 5432)                     |
| `LDP_DB_NAME`            | PostgreSQL database name (default: ldp)                      |
| `LDP_DB_USERNAME_SECRET` | Secrets Manager secret name for database username (required) |
| `LDP_DB_PASSWORD_SECRET` | Secrets Manager secret name for database password (required) |
| `LOG_LEVEL`              | Logging level (default: DEBUG)                               |

## Path Mapping

Landing path → Bronze path:

- `landing/reference/onspd/2024/12/02/file.csv` → `bronze/reference/onspd/2024/12/02/file.csv`

## Database Table

Table: `ldp_file_ingest_log`

| Column        | Type      | Description                                      |
|---------------|-----------|--------------------------------------------------|
| `dataset_key` | VARCHAR   | Primary key part 1 (e.g., `reference/onspd`)     |
| `file_name`   | VARCHAR   | Primary key part 2                               |
| `checksum`    | VARCHAR   | SHA256 checksum                                  |
| `status`      | VARCHAR   | Processing status (`bronze_done`, `silver_done`) |
| `ingested_at` | TIMESTAMP | Ingestion timestamp                              |
| `rows_bronze` | INTEGER   | Optional row count                               |

## Idempotency

Re-uploading the same file content will not create additional copies in Bronze.

