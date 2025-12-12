# Checksum Lambda

Lambda function triggered by S3 events that processes reference data files arriving in the Landing zone.

## Overview

This function:

1. Calculates SHA256 checksum for files arriving in Landing bucket
2. Checks database for existing records (duplicate detection)
3. Copies file to Bronze bucket if new/changed
4. Updates database record with status `bronze_done`
5. Removes duplicate from Landing bucket (or moves new file)

## Processing Flow

```
S3 Landing (ObjectCreated) → Checksum Lambda → S3 Bronze
                                   ↓
                            PostgreSQL (location.ldp_file_ingest_log)
```

## Trigger

| Setting | Value                |
|---------|----------------------|
| Event   | `S3:ObjectCreated:*` |
| Bucket  | `ldp-zone-a-landing` |
| Prefix  | `landing/reference/` |

## Path Structure

**Expected input path format:**

```
landing/reference/{dataset}/{year}/{month}/{day}/{filename}
```

**Example mapping:**

```
landing/reference/onspd/2024/12/02/ONSPD_DEC_2024.csv
    → bronze/reference/onspd/2024/12/02/ONSPD_DEC_2024.csv
```

## Environment Variables

| Variable                 | Required | Default | Description                         |
|--------------------------|----------|---------|-------------------------------------|
| `BRONZE_BUCKET`          | Yes      | -       | Destination S3 bucket name          |
| `LDP_DB_HOST`            | Yes      | -       | PostgreSQL database host            |
| `LDP_DB_PORT`            | No       | `5432`  | PostgreSQL database port            |
| `LDP_DB_NAME`            | No       | `ldp`   | PostgreSQL database name            |
| `LDP_DB_USERNAME_SECRET` | Yes      | -       | Secrets Manager key for DB username |
| `LDP_DB_PASSWORD_SECRET` | Yes      | -       | Secrets Manager key for DB password |
| `LOG_LEVEL`              | No       | `DEBUG` | Logging level                       |

## Database Schema

Schema: `location`  
Table: `ldp_file_ingest_log`

```sql
CREATE TABLE location.ldp_file_ingest_log (
    dataset_key TEXT NOT NULL,  -- e.g., 'reference/onspd'
    file_name TEXT NOT NULL,
    checksum TEXT NOT NULL,       -- SHA256 hex
    status TEXT NOT NULL,         -- 'bronze_done', 'silver_done'
    ingested_at TIMESTAMP NOT NULL,
    rows_bronze INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_key, file_name)
);
```

## Database Initialisation

Run the DDL script to create schema, table and user:

```bash
psql -d ldp -f location/data/init/ddl/create_schema_location.sql
```

After running, set the password for the role and store it in AWS Secrets Manager:

```sql
ALTER ROLE location_writer WITH PASSWORD 'your-secure-password';
```

This creates:

- `location` schema
- `ldp_file_ingest_log` table
- `location_writer` role with INSERT/UPDATE/SELECT permissions (no DELETE)

> **TODO (Phase 2)**: Extract SQL scripts to a dedicated database migrations repository with proper versioning
> and migration tooling (e.g., Liquibase). Manual script execution is acceptable for Discovery
> phase but not suitable for production deployments.

## Duplicate Detection

The function is **idempotent**:

- If file with same `dataset_key` + `file_name` + `checksum` exists with status `bronze_done` or `silver_done` → file is
  deleted from Landing (duplicate)
- If checksum differs → file is copied to Bronze, record is updated

## Response

```json
{
  "status": "success",
  "dataset_key": "reference/onspd",
  "file_name": "ONSPD_DEC_2024.csv",
  "checksum": "abc123...",
  "bronze_key": "bronze/reference/onspd/2024/12/02/ONSPD_DEC_2024.csv"
}
```

**Skipped (duplicate):**

```json
{
  "status": "skipped",
  "reason": "duplicate",
  "checksum": "abc123..."
}
```

> **Note**: The original HLD specified DynamoDB for storing ingestion records. However, due to the use of RDS for
> matching and FHIR purposes, the decision was made to store this data in RDS as well.

