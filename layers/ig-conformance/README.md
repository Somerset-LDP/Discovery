# IG Conformance Layer

Filters raw healthcare data to retain only records for patients in the approved cohort.

## Purpose

This layer is the first processing step after raw data ingestion. It ensures:

- **Cohort filtering** - Only records for patients in the pseudonymised cohort are retained
- **Data minimisation** - All non-cohort records are discarded immediately
- **Short-lived storage** - Data is deleted after processing by the next
  layer ([Pseudonymised](../pseudonymised/README.md))

## Processing Flow

```
Raw Data (S3) → IG Conformance Lambda → Filtered Data (S3) → [Pseudonymised Layer]
                        ↓
                  Cohort Store (pseudonymised NHS numbers)
                        ↓
                  Pseudonymisation Lambda (encrypt NHS for comparison)
```

1. Read input file from S3
2. Extract NHS numbers from records
3. Encrypt NHS numbers via Pseudonymisation Lambda
4. Compare encrypted NHS against cohort store
5. Retain only matching records
6. Write filtered output to S3
7. Delete source file

## Event Format

```json
{
  "input_path": "s3://bucket/bronze/gp/2024/01/15/patients.csv",
  "output_path": "s3://bucket/ig-conformance",
  "feed_type": "gp"
}
```

| Parameter     | Required | Description                                                |
|---------------|----------|------------------------------------------------------------|
| `input_path`  | Yes      | Full S3 path to input CSV file                             |
| `output_path` | Yes      | Base S3 path for output (date path appended automatically) |
| `feed_type`   | Yes      | Feed type: `gp` or `sft`                                   |

### Response

```json
{
  "statusCode": 200,
  "body": {
    "request_id": "abc-123",
    "message": "GP pipeline executed successfully",
    "feed_type": "gp",
    "input_records": 15000,
    "output_records": 4523,
    "output_file": "s3://bucket/ig-conformance/gp_feed/2024/01/15/patients.csv"
  }
}
```

## Feed Configurations

| Feed  | NHS Column | Metadata Rows  | Preserve Metadata |
|-------|------------|----------------|-------------------|
| `gp`  | Column 0   | 2 rows skipped | Yes               |
| `sft` | Column 1   | None           | No                |

**Output path structure:** `{output_path}/{feed_type}_feed/YYYY/MM/DD/{original_filename}`

## Environment Variables

| Variable                                | Required | Description                                                        |
|-----------------------------------------|----------|--------------------------------------------------------------------|
| `COHORT_STORE`                          | Yes      | S3 path to cohort file (CSV, no header, pseudonymised NHS numbers) |
| `PSEUDONYMISATION_LAMBDA_FUNCTION_NAME` | Yes      | Name of Pseudonymisation Lambda                                    |
| `KMS_KEY_ID`                            | Yes      | ARN of KMS key for output encryption                               |
| `LOG_LEVEL`                             | No       | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)   |
| `PSEUDONYMISATION_BATCH_SIZE`           | No       | Max NHS numbers per pseudonymisation request (default: 10000)      |
| `SKIP_ENCRYPTION`                       | No       | Skip pseudonymisation calls (testing only, not for production)     |

### Cohort Store Format

CSV file with pseudonymised NHS numbers (one per line, no header):

```
YWJjZGVmZ2hpamts...
cXJzdHV2d3h5ejEy...
Nzg5MGFiY2RlZmdo...
```

## Project Structure

```
ig-conformance/
├── aws/
│   └── lambdas/
│       └── handler.py      # Lambda entry point
├── common/
│   ├── cohort_membership.py  # Cohort store reader
│   └── filesystem.py         # S3/local file operations
├── pipeline/
│   ├── conformance_processor.py  # Core filtering logic
│   └── feed_config.py            # Feed type configurations
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
  -t ig_conformance:latest \
  -f Dockerfile .
```

### Local Testing

```bash
# Start container
docker run -d --platform linux/amd64 -p 9000:8080 ig_conformance:latest

# Test invocation
curl "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"input_path": "s3://test/input.csv", "output_path": "s3://test/output", "feed_type": "gp"}'
```

### Corporate Network Build (ZScaler)

When building behind corporate proxies, include SSL certificates:

```bash
docker buildx build \
  --secret id=ssl-certs,src=/etc/ssl/certs/ca-certificates.crt \
  --platform linux/amd64 \
  --provenance=false \
  -t ig_conformance:latest \
  -f Dockerfile .
```
