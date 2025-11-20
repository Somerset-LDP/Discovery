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

**Storage**: Object store (S3) with feed-first hierarchy:
```
pseudonymised/
├── gp_feed/YYYY/MM/DD/
│   ├── raw/          # GP practice data, PII pseudonymised
│   └── calculated/   # Derived values (age, etc.)
└── sft_feed/YYYY/MM/DD/
    ├── raw/          # SFT data, PII pseudonymised
    └── calculated/   # Derived values (age, etc.)
```

**Output Path Pattern**: `{feed_type}_feed/{year}/{month}/{day}/raw/patient_{timestamp}.csv`

Examples:
- GP: `gp_feed/2025/11/20/raw/patient_20251120_143045_123456.csv`
- SFT: `sft_feed/2025/11/20/raw/patient_20251120_143045_123456.csv`

## Environment Variables
Required Lambda environment variables:
- `PSEUDONYMISATION_LAMBDA_FUNCTION_NAME` - Name/ARN of pseudonymisation Lambda function
- `KMS_KEY_ID` - KMS key ID for S3 server-side encryption

## Lambda Event Structure
The Lambda function accepts the following event parameters:

```json
{
  "input_s3_bucket": "source-bucket-name",
  "input_prefix": "path/to/raw/files/",
  "output_s3_bucket": "destination-bucket-name",
  "feed_type": "gp"
}
```

**Event Parameters:**
- `input_s3_bucket` (required) - S3 bucket containing raw CSV files to process
- `input_prefix` (required) - S3 prefix/path where input files are located
- `output_s3_bucket` (required) - S3 bucket where pseudonymised files will be written
- `feed_type` (required) - Type of data feed: `"gp"` or `"sft"`

**Example Lambda Event - GP Feed:**
```json
{
  "input_s3_bucket": "somerset-raw-data",
  "input_prefix": "gp_feed/2025/11/20/",
  "output_s3_bucket": "somerset-pseudonymised",
  "feed_type": "gp"
}
```

**Example Lambda Event - SFT Feed:**
```json
{
  "input_s3_bucket": "somerset-raw-data",
  "input_prefix": "sft_feed/2025/11/20/",
  "output_s3_bucket": "somerset-pseudonymised",
  "feed_type": "sft"
}
```

## Feed Configuration

The pipeline supports multiple feed types via `feed_config.py`. Each feed has its own configuration defining:

### GP Feed (`feed_type="gp"`)
- **Metadata handling**: Preserves 2 header rows from source files
- **Column names**: Uses capitalized names with spaces (e.g., 'NHS Number', 'Given Name')
- **Date format**: `%d-%b-%Y` (e.g., 15-Jan-1985)
- **Output path**: `gp_feed/YYYY/MM/DD/raw/patient_timestamp.csv`

**GP Feed Fields:**
- `NHS Number` → pseudonymised as `nhs_number`
- `Given Name` → pseudonymised as `given_name`
- `Family Name` → pseudonymised as `family_name`
- `Date of Birth` → pseudonymised as `date_of_birth`
- `Gender` → pseudonymised as `gender`
- `Postcode` → pseudonymised as `postcode`

### SFT Feed (`feed_type="sft"`)
- **Metadata handling**: No metadata preservation (skiprows=0)
- **Column names**: Uses lowercase with underscores (e.g., 'nhs_number', 'first_name')
- **Date format**: `%Y-%m-%d` (e.g., 1985-01-15)
- **Output path**: `sft_feed/YYYY/MM/DD/raw/patient_timestamp.csv`

**SFT Feed Fields:**
- `nhs_number` → pseudonymised as `nhs_number`
- `first_name` → pseudonymised as `first_name`
- `last_name` → pseudonymised as `last_name`
- `date_of_birth` → pseudonymised as `date_of_birth`
- `sex` → pseudonymised as `sex`
- `postcode` → pseudonymised as `postcode`


## Data Validation Rules
Records are validated before pseudonymisation using rules from `feed_config`. Invalid records are logged and removed:

### Common Validation Rules (Both Feeds):
1. **NHS Number**: Must be valid 10-digit number with correct check digit (Modulus 11)
2. **Name fields**: Cannot be null, empty, or whitespace
3. **Postcode**: Must match valid UK postcode format (e.g., SW1A 1AA)

### Feed-Specific Rules:

#### GP Feed:
- **Date of Birth**: Format `DD-MMM-YY` (e.g., 15-Jan-85)
- **Gender**: Valid values are 'Male', 'Female', 'Indeterminate'

#### SFT Feed:
- **Date of Birth**: Format `YYYY-MM-DD` (e.g., 1985-01-15)
- **Sex**: Valid values are '1' (Male), '2' (Female), '9' (Not known/Not specified) - per [NHS Data Dictionary Person Stated Gender Code](https://v3.datadictionary.nhs.uk/data_dictionary/attributes/p/person/person_stated_gender_code_de.asp)

**Note:** Invalid records do NOT cause pipeline failure - they are excluded from processing with details logged.
