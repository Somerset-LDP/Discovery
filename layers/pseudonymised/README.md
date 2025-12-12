# Pseudonymised Layer

**Purpose**: A safe, minimal representation of raw input feeds with immediate PII protection.

## Characteristics

- **Immediate pseudonymisation**: PII is encrypted at ingestion - never persisted in raw form
- **No interpretation**: Data conflicts and business rules are not resolved at this stage
- **Feed-specific structure**: Data retains original feed formats for auditability
- **Source cleanup**: Input files are deleted after successful processing

## Processing Flow

```
IG Conformance (S3) → Pseudonymised Lambda → Pseudonymised Data (S3)
                              ↓
                    Pseudonymisation Lambda
                    (encrypt all PII fields)
```

1. List all CSV files in input prefix
2. Read and validate each file
3. Pseudonymise all PII fields via batch encryption
4. Write output to date-partitioned S3 path
5. Delete source file

## Storage Structure

```
pseudonymised/
├── gp_feed/YYYY/MM/DD/
│   ├── raw/          # GP practice data, PII pseudonymised
│   └── calculated/   # Derived values (age, etc.)
└── sft_feed/YYYY/MM/DD/
    ├── raw/          # SFT data, PII pseudonymised
    └── calculated/   # Derived values (age, etc.)
```

**Output filename pattern**: `patient_{YYYYMMDD}_{HHMMSS}_{microseconds}.csv`

## Lambda Event Format

```json
{
  "input_s3_bucket": "somerset-ig-conformance",
  "input_prefix": "gp_feed/2025/01/15/",
  "output_s3_bucket": "somerset-pseudonymised",
  "feed_type": "gp"
}
```

| Parameter          | Required | Description                             |
|--------------------|----------|-----------------------------------------|
| `input_s3_bucket`  | Yes      | S3 bucket containing input CSV files    |
| `input_prefix`     | Yes      | S3 prefix where input files are located |
| `output_s3_bucket` | Yes      | S3 bucket for pseudonymised output      |
| `feed_type`        | Yes      | Feed type: `gp` or `sft`                |

### Response

```json
{
  "statusCode": 200,
  "body": {
    "message": "Pseudonymisation completed successfully",
    "files_processed": 3,
    "total_records": 4523,
    "output_prefix": "gp_feed/2025/01/15/raw/"
  }
}
```

## Environment Variables

| Variable                                | Required | Description                               |
|-----------------------------------------|----------|-------------------------------------------|
| `PSEUDONYMISATION_LAMBDA_FUNCTION_NAME` | Yes      | Name/ARN of Pseudonymisation Lambda       |
| `KMS_KEY_ID`                            | Yes      | KMS key ARN for S3 server-side encryption |

## Feed Configurations

### GP Feed (`feed_type="gp"`)

| Setting       | Value                             |
|---------------|-----------------------------------|
| Metadata rows | 2 (preserved in output)           |
| Date format   | `DD-Mon-YY` (e.g., `15-Jan-85`)   |
| Gender values | `Male`, `Female`, `Indeterminate` |

**Fields pseudonymised:**
| Source Column | Pseudonymisation Field |
|---------------|------------------------|
| `NHS Number` | `nhs_number` |
| `Given Name` | `given_name` |
| `Family Name` | `family_name` |
| `Date of Birth` | `date_of_birth` |
| `Gender` | `gender` |
| `Postcode` | `postcode` |

### SFT Feed (`feed_type="sft"`)

| Setting       | Value                                         |
|---------------|-----------------------------------------------|
| Metadata rows | None                                          |
| Date format   | `YYYY-MM-DD` (e.g., `1985-01-15`)             |
| Sex values    | `1` (Male), `2` (Female), `9` (Not specified) |

**Fields pseudonymised:**
| Source Column | Pseudonymisation Field |
|---------------|------------------------|
| `nhs_number` | `nhs_number` |
| `first_name` | `first_name` |
| `last_name` | `last_name` |
| `date_of_birth` | `date_of_birth` |
| `sex` | `sex` |
| `postcode` | `postcode` |

## Data Validation

Records are validated before pseudonymisation. Invalid records are **excluded** (not failed) with details logged.

### Validation Rules

| Field         | Rule                                         |
|---------------|----------------------------------------------|
| NHS Number    | 10 digits with valid Modulus 11 check digit  |
| Name fields   | Non-empty string                             |
| Postcode      | Valid UK format (e.g., `SW1A 1AA`, `M1 1AA`) |
| Date of Birth | Matches feed-specific format                 |
| Gender/Sex    | Matches feed-specific allowed values         |

**Note**: Sex codes
follow [NHS Data Dictionary Person Stated Gender Code](https://v3.datadictionary.nhs.uk/data_dictionary/attributes/p/person/person_stated_gender_code_de.asp).

## Project Structure

```
pseudonymised/
├── handler.py           # Lambda entry point
├── feed_config.py       # Feed type configurations
├── validation_utils.py  # Record validation logic
├── aws_utils.py         # S3 and Lambda operations
├── requirements.txt
└── README.md
```
