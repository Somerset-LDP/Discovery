# Canonical layer

**Purpose**: The source of truth for clean, consistent, canonical data models. This is the business-ready foundation for all downstream analytics and processing.

**Characteristics**:
- **Conflict resolution**: When multiple feeds provide conflicting data, business rules determine truth
- **Canonical models**: Standard patient, encounter, observation entities reflecting business semantics
- **Data quality enforcement**: Type validation, reference integrity, consistency rules
- **Feed-agnostic**: Data structure no longer tied to source system quirks
- **FHIR integration**: Code system validation and terminology mapping
- **Multi-feed support**: Configurable processing for different feed types (GP, SFT, etc.)

**Storage**: Relational database with normalized schema

## Feed Configuration

The canonical layer uses a feed configuration system (`canonical_feed_config.py`) to handle different data sources with varying structures and validation rules.

### Supported Feed Types

#### GP Feed
- **Metadata rows to skip**: 2
- **Date format**: `%d-%b-%y` (e.g., "01-Jan-50")
- **Has measurements**: Yes (height in cm, weight in kg)
- **Required fields**: nhs_number, given_name, family_name, date_of_birth, postcode, sex
- **CSV column mappings** (position → database column):
  - 0: nhs_number
  - 1: given_name
  - 2: family_name
  - 3: date_of_birth
  - 4: postcode
  - 6: sex
  - 7: height_cm
  - 9: height_observation_time
  - 10: weight_kg
  - 12: weight_observation_time
- **Auxiliary columns**: first_line_of_address (5), height_unit (8), weight_unit (11), consultation_id (13), consultation_date (14), consultation_time (15), consultation_type (16), user_type (17)

#### SFT Feed
- **Metadata rows to skip**: 0
- **Date format**: `%Y-%m-%d` (e.g., "1950-01-01")
- **Has measurements**: No
- **Required fields**: nhs_number, given_name, family_name, date_of_birth, sex, postcode
- **CSV column mappings** (position → database column):
  - 1: nhs_number
  - 2: given_name
  - 3: family_name
  - 4: date_of_birth
  - 5: sex
  - 6: postcode
- **Auxiliary columns**: pas_number (0), first_line_of_address (7)

### Configuration Structure

Each feed configuration (`FeedConfig` dataclass in `canonical_feed_config.py`) includes:
- **feed_type**: Identifier (e.g., "gp", "sft")
- **metadata_rows_to_skip**: Number of header rows to skip in CSV
- **db_columns**: Dict mapping database column names to CSV column positions
- **csv_auxiliary_columns**: Dict of auxiliary CSV columns not directly saved to database
- **validation_rules**: Dict with:
  - `required_patient_fields`: List of mandatory fields
  - `valid_date_format`: Date format string for parsing
  - `has_measurements`: Boolean indicating if feed includes height/weight
  - Additional feed-specific validation rules

### Lambda Event Format

The Lambda function expects the following event structure:

```json
{
  "feed_type": "gp",
  "input_path": "s3://bucket-name/path/to/file.csv"
}
```

**Required parameters**:
- `feed_type` (string): Type of feed to process ("gp" or "sft")
- `input_path` (string): S3 path to the input CSV file

### Environment Variables

- `OUTPUT_DB_HOST`: Database host
- `OUTPUT_DB_PORT`: Database port (default: 5432)
- `OUTPUT_DB_NAME`: Database name (default: "ldp")
- `OUTPUT_DB_USERNAME_SECRET`: AWS Secrets Manager key for DB username
- `OUTPUT_DB_PASSWORD_SECRET`: AWS Secrets Manager key for DB password
- `OUTPUT_DB_SCHEMA`: Target schema (default: "canonical")
- `OUTPUT_DB_TABLE`: Target table (default: "patient")
- `LOG_LEVEL`: Logging level (default: "INFO")


## Project strucutre

```
canonical/
├── README.md
│   └─ Project documentation and usage instructions.
├── aws/
│   └─ AWS specific code e.g. Lambdas to run pipelines in an AWS environment
├── pipeline/
|    └─ Data ingestion pipelines
└── tests/
    └─ Unit and Integration tests built with Pytest
```

## Build & Test

### Prerequisites
- Docker with buildx support
- Python 3.12+
- pytest for running tests

## Building the Docker image

The Lambda function is packaged as a Docker container for deployment to AWS Lambda.

For local development and testing -
```bash
docker buildx build \
  --platform linux/amd64 \
  --provenance=false \
  -t canonical_layer:latest \
  -f Dockerfile .
```

Smoke testing the image- 
```bash
docker run -d --platform linux/amd64 -p 9000:8080 canonical_layer:latest

curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{
  "feed_type": "gp",
  "input_path": "s3://test-bucket/sample.csv"
}'
```

#### Corporate Network Build (ZScaler)
When building behind corporate firewalls or proxies, include SSL certificates:
Note that the secret id must be named `ssl-certs` and points to the path of your corporate SSL cert

```bash
docker buildx build \
  --secret id=ssl-certs,src=/etc/ssl/certs/ca-certificates.crt \
  --platform linux/amd64 \
  --provenance=false \
  -t canonical_layer:latest \
  -f Dockerfile .
```