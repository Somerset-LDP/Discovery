# Pseudonymised layer

**Purpose**: A safe, minimal representation of raw input feeds with immediate PII protection.

**Characteristics**:
- **Immediate pseudonymisation**: PII is stripped or hashed at ingestion - never persisted in raw form
- **No interpretation**: Data conflicts and business rules are not resolved at this stage  
- **Feed-specific structure**: Data retains original feed formats for auditability
- **Minimal enrichment**: Only computations requiring PII input (e.g., age calculation from DOB)
- **Two output types**:
  - *Raw-like pseudonymised*: Structurally close to source but PII-safe
  - *Calculated pseudonymised*: Derived values computed before PII disposal

**Storage**: Object store (S3/GCS/Azure) with feed-first hierarchy:
```
pseudonymised/
├── feed_a/YYYY/MM/DD/
│   ├── raw/          # Near-original structure, PII removed
│   └── calculated/   # Age, derived demographics
└── feed_b/YYYY/MM/DD/
    ├── raw/          # Near-original structure, PII removed
    └── calculated/   # Age, derived demographics
```

## Environment Variables
Required environment variables:
- `INPUT_S3_BUCKET` - Source S3 bucket for raw CSV files
- `INPUT_PREFIX` - S3 prefix for input files
- `OUTPUT_S3_BUCKET` - Destination S3 bucket for pseudonymised data
- `PSEUDONYMISATION_LAMBDA_FUNCTION_NAME` - Name/ARN of pseudonymisation Lambda
- `KMS_KEY_ID` - KMS key for S3 encryption

## Data Validation Rules
Records are validated before pseudonymisation. Invalid records are logged (field name, error) and removed:

1. NHS Number: Must be valid 10-digit number with correct check digit (Modulus 11)
2. Given Name: Cannot be null, empty, or whitespace
3. Family Name: Cannot be null, empty, or whitespace
4. Gender: Cannot be null, empty, or whitespace, allowed values: 'Male', 'Female', 'Indeterminate'
5. Date of Birth: Must be valid date in format DD-MMM-YY
6. Postcode: Must be valid UK postcode format

Invalid records do NOT cause pipeline failure - they are simply excluded from processing.
