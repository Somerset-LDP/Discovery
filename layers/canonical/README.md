# Canonical Layer

**Purpose**: The source of truth for clean, consistent, canonical data models. This is the business-ready foundation for
all downstream analytics and processing.

## Characteristics

- **Conflict resolution**: When multiple feeds provide conflicting data, business rules determine truth
- **Canonical models**: Standard patient entity reflecting business semantics
- **Data quality enforcement**: Type validation, reference integrity, consistency rules
- **Feed-agnostic**: Data structure no longer tied to source system quirks
- **Deduplication**: Multiple records for same NHS number are merged (first occurrence wins)

## Processing Flow

```
Pseudonymised (S3) → Canonical Lambda → PostgreSQL Database
                           ↓
                    Validation & Transformation
                    (parse, validate, dedupe)
```

1. Read CSV file from S3
2. Parse columns according to feed configuration
3. Validate required fields and data formats
4. Deduplicate by NHS number (first record wins on conflict)
5. Write to PostgreSQL `canonical.patient` table

## Lambda Event Format

```json
{
  "feed_type": "gp",
  "input_path": "s3://bucket-name/pseudonymised/gp_feed/2025/01/15/patient.csv"
}
```

| Parameter    | Required | Description               |
|--------------|----------|---------------------------|
| `feed_type`  | Yes      | Feed type: `gp` or `sft`  |
| `input_path` | Yes      | S3 path to input CSV file |

### Response

```json
{
  "statusCode": 200,
  "body": {
    "message": "GP pipeline execution completed successfully",
    "request_id": "abc-123",
    "feed_type": "gp",
    "records_processed": 4523,
    "records_stored": 4102
  }
}
```

## Environment Variables

| Variable                    | Required | Default     | Description                         |
|-----------------------------|----------|-------------|-------------------------------------|
| `OUTPUT_DB_HOST`            | Yes      | -           | PostgreSQL host                     |
| `OUTPUT_DB_PORT`            | No       | `5432`      | PostgreSQL port                     |
| `OUTPUT_DB_NAME`            | No       | `ldp`       | Database name                       |
| `OUTPUT_DB_USERNAME_SECRET` | Yes      | -           | Secrets Manager key for DB username |
| `OUTPUT_DB_PASSWORD_SECRET` | Yes      | -           | Secrets Manager key for DB password |
| `OUTPUT_DB_SCHEMA`          | No       | `canonical` | Target schema                       |
| `OUTPUT_DB_TABLE`           | No       | `patient`   | Target table                        |
| `LOG_LEVEL`                 | No       | `INFO`      | Logging level                       |

## Feed Configurations

### GP Feed (`feed_type="gp"`)

| Setting          | Value                           |
|------------------|---------------------------------|
| Metadata rows    | 2 (skipped)                     |
| Date format      | `DD-Mon-YY` (e.g., `01-Jan-50`) |
| Has measurements | Yes (height, weight)            |

**Column mappings (CSV position → DB column):**

| Position | DB Column                 | Required |
|----------|---------------------------|----------|
| 0        | `nhs_number`              | Yes      |
| 1        | `given_name`              | Yes      |
| 2        | `family_name`             | Yes      |
| 3        | `date_of_birth`           | Yes      |
| 4        | `postcode`                | Yes      |
| 6        | `sex`                     | Yes      |
| 7        | `height_cm`               | No       |
| 9        | `height_observation_time` | No       |
| 10       | `weight_kg`               | No       |
| 12       | `weight_observation_time` | No       |

### SFT Feed (`feed_type="sft"`)

| Setting          | Value                             |
|------------------|-----------------------------------|
| Metadata rows    | None                              |
| Date format      | `YYYY-MM-DD` (e.g., `1950-01-01`) |
| Has measurements | No                                |

**Column mappings (CSV position → DB column):**

| Position | DB Column       | Required |
|----------|-----------------|----------|
| 1        | `nhs_number`    | Yes      |
| 2        | `given_name`    | Yes      |
| 3        | `family_name`   | Yes      |
| 4        | `date_of_birth` | Yes      |
| 5        | `sex`           | Yes      |
| 6        | `postcode`      | Yes      |

## Database Schema

Target table: `canonical.patient`

```sql
CREATE TABLE canonical.patient (
    nhs_number TEXT NOT NULL,
    given_name TEXT NOT NULL,
    family_name TEXT NOT NULL,
    date_of_birth TEXT NOT NULL,
    postcode TEXT NOT NULL,
    sex TEXT,
    height_cm NUMERIC(5,2),
    height_observation_time TIMESTAMP,
    weight_kg NUMERIC(5,2),
    weight_observation_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Project Structure

```
canonical/
├── aws/
│   └── lambdas/
│       └── handler.py        # Lambda entry point
├── data/
│   └── init/
│       └── ddl/
│           └── create_schema_canonical.sql  # Database schema DDL
├── pipeline/
│   ├── canonical_processor.py    # Core processing logic
│   └── canonical_feed_config.py  # Feed configurations
├── tests/
├── Dockerfile
├── requirements.txt
└── README.md
```

## Build & Deploy

### Building Docker Image

```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  -t canonical_layer:latest \
  -f Dockerfile .
```

### Local Testing

```bash
# Start container
docker run -d --platform linux/amd64 -p 9000:8080 canonical_layer:latest

# Test invocation
curl "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"feed_type": "gp", "input_path": "s3://test-bucket/sample.csv"}'
```

### Corporate Network Build (ZScaler)

```bash
docker buildx build \
  --secret id=ssl-certs,src=/etc/ssl/certs/ca-certificates.crt \
  --platform linux/amd64 \
  --provenance=false \
  -t canonical_layer:latest \
  -f Dockerfile .
```

## Database Initialisation

Run the DDL script to create the schema and table:

```bash
psql -d ldp -v user_password="'YourSecurePassword'" \
  -f data/init/ddl/create_schema_canonical.sql
```

This creates:

- `canonical` schema
- `patient` table
- `canonical_writer` role with INSERT/UPDATE/SELECT permissions (no DELETE)
