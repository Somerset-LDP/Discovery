# Cohort Data Processing Lambda

AWS Lambda function for identifying and pseudonymising the patient cohort that will be used across all subsequent processing layers.

## Purpose

This Lambda function serves as the **entry point** for the data processing pipeline. It:

1. **Identifies the cohort** - Finds the intersection of patients between:
   - **SFT (Overnight Stay)** data - patients who had overnight hospital stays
   - **GP (Registered Patient)** data - patients registered with participating GP practices

2. **Pseudonymises NHS numbers** - Replaces real NHS numbers with encrypted pseudonyms using the pseudonymisation Lambda service

3. **Creates the master cohort list** - Outputs a single CSV file with pseudonymised NHS numbers that will be used by all downstream processing layers


## Environment Variables

Required environment variables:

```bash
# S3 Paths
S3_SFT_FILE_PREFIX=bucket/uploads/overnight-stay/files/
S3_SFT_CHECKSUM_PREFIX=bucket/uploads/overnight-stay/checksums/
S3_GP_FILES_PREFIX=bucket/uploads/registered-patient/files/
S3_GP_CHECKSUMS_PREFIX=bucket/uploads/registered-patient/checksums/
S3_COHORT_KEY=bucket/cohort/cohort.csv

# AWS Services
KMS_KEY_ID=arn:aws:kms:region:account:key/key-id
PSEUDONYMISATION_LAMBDA_FUNCTION_NAME=pseudonymisation-lambda
```