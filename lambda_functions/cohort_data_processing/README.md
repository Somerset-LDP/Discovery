# Cohort Data Processing Lambda

AWS Lambda function for identifying and pseudonymising the patient cohort that will be used across all subsequent processing layers.

## Purpose
This Lambda function serves as the entry point for the data processing pipeline. It supports two processing modes
controlled by a feature flag:

### Mode SFT+GP Intersection (PROCESS_SFT_FILES=1)
1. Loads SFT data - Reads overnight hospital stay patient records
2. Loads GP data - Reads registered patient records from all GP practices
3. Calculates intersection - Identifies patients present in both SFT and any GP file
4. Pseudonymises NHS numbers - Encrypts NHS numbers using the pseudonymisation service
5. Outputs cohort - Writes pseudonymised NHS numbers to S3 (no size limit)

### Mode GP-Only with Sampling (PROCESS_SFT_FILES=0)
1. Loads GP data - Reads registered patient records from all GP practices
2. Applies proportional sampling - Takes up to 5000 records, distributed evenly across GP files
3. Pseudonymises NHS numbers - Encrypts sampled NHS numbers using the pseudonymisation service
4. Outputs cohort - Writes pseudonymised NHS numbers to S3 (max 5000 records)

## Processing Modes
| Feature       | SFT+GP Mode                                | GP-Only Mode                        |
|---------------|--------------------------------------------|-------------------------------------|
| Feature Flag  | `PROCESS_SFT_FILES=1`                      | `PROCESS_SFT_FILES=0`               |
| Data Sources  | SFT + GP files                             | GP files only                       |
| Logic         | Intersection of SFT with union of GP files | Proportional sampling from GP files |
| Size Limit    | None (full intersection)                   | 5000 records max                    |
| SFT Variables | Required                                   | Not required                        |

## Environment Variables
### Shared Configuration (Both Modes)

```bash
# Feature flag - determines processing mode
PROCESS_SFT_FILES=0  # 0=GP-only with sampling, 1=SFT+GP intersection

# Output location
S3_COHORT_KEY=bucket/cohort/cohort.csv

# AWS Services
KMS_KEY_ID=arn:aws:kms:region:account:key/key-id
PSEUDONYMISATION_LAMBDA_FUNCTION_NAME=pseudonymisation-lambda

# GP data sources (required in both modes)
S3_GP_FILES_PREFIX=bucket/uploads/registered-patient/files/
S3_GP_CHECKSUMS_PREFIX=bucket/uploads/registered-patient/checksums/
```

### Mode SFT+GP Intersection (Required only when PROCESS_SFT_FILES=1)

```bash
# SFT data sources
S3_SFT_FILE_PREFIX=bucket/uploads/overnight-stay/files/
S3_SFT_CHECKSUM_PREFIX=bucket/uploads/overnight-stay/checksums/
```

## Input Data Format
All input files must be:

- CSV format with NHS numbers in the first column
- Accompanied by SHA256 checksum files
- Named consistently (e.g., `data.csv` → `data.sha256`)

## Output
Produces a single CSV file at `S3_COHORT_KEY` containing pseudonymised NHS numbers (one per line, no header).

## Cleanup
After successful processing, all source files and checksums are deleted from S3.
