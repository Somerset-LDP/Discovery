# Synthetic Data Generator

Utilities for generating synthetic NHS test data and uploading to S3.

## Modules

### `data_generators.py`

Generates synthetic patient data:

- **`generate_valid_nhs_number()`** - Creates NHS numbers with correct Modulus 11 checksum
- **`generate_invalid_nhs_number()`** - Creates intentionally invalid NHS numbers (wrong checksum)
- **`generate_random_name()`** - Generates random first names using syllable combinations
- **`generate_random_surname()`** - Generates random surnames using syllable combinations
- **`generate_random_dob()`** - Generates random date of birth in YYYY-MM-DD format
- **`generate_random_postcode()`** - Generates random UK-style postcodes

### `synthetic_data_utils.py`

Main utilities for creating and uploading test data to S3:

- **`generate_nhs_numbers()`** - Generate a list of valid and invalid NHS numbers
- **`create_csv_file()`** - Create a CSV file with NHS numbers
- **`generate_sha256_checksum()`** - Generate SHA256 checksum file in `sha256sum` format
- **`upload_to_s3()`** - Upload file to S3 with KMS encryption
- **`upload_file_and_checksum()`** - Upload both data file and checksum to S3
- **`generate_and_upload_sft_data()`** - Generate and upload SFT (overnight stay) files
- **`generate_and_upload_gp_data()`** - Generate and upload GP (registered patient) files
- **`generate_and_upload_all_test_data()`** - Generate both with configurable overlap

## Required Environment Variables

```bash
# AWS Configuration
KMS_KEY_ID=arn:aws:kms:region:account:key/key-id
S3_BUCKET=your-bucket-name

# SFT Configuration
SFT_FILES_PREFIX=uploads/overnight-stay/files/
SFT_CHECKSUMS_PREFIX=uploads/overnight-stay/checksums/

# GP Configuration
GP_FILES_PREFIX=uploads/registered-patient/files/
GP_CHECKSUMS_PREFIX=uploads/registered-patient/checksums/

# Data Generation Settings
NUM_SFT_FILES=1
NUM_GP_FILES=20
VALID_NHS_PER_FILE=2000
INVALID_NHS_PER_FILE=300
GP_SFT_OVERLAP_RATIO=0.4

# Local Settings
LOCAL_TMP_DIR=.
```

## Usage

```python
# Option 1: Generate both SFT and GP data
generate_and_upload_all_test_data()

# Option 2: Generate only SFT data
generate_and_upload_sft_data()

# Option 3: Generate only GP data (without SFT overlap)
generate_and_upload_gp_data()
```

## Features

- ✅ Generates valid NHS numbers with correct Modulus 11 checksums
- ✅ Creates SHA256 checksums in `sha256sum` format
- ✅ Uploads to S3 with KMS encryption
- ✅ Configurable SFT/GP overlap ratio
- ✅ Automatic local file cleanup after upload
- ✅ Separate functions for SFT and GP data generation

