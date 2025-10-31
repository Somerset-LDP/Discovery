# Synthetic Data Generator

Utilities for generating synthetic NHS test data and uploading to S3.

## Modules

### `data_generators.py`
Generates valid and invalid NHS numbers:
- **`generate_valid_nhs_number()`** - Creates NHS numbers with correct Modulus 11 checksum
- **`generate_invalid_nhs_number()`** - Creates intentionally invalid NHS numbers (wrong length, format, or checksum)

### `synthetic_data_utils.py`
Main utilities for creating and uploading test data to S3:
- **`generate_and_upload_sft_data()`** - Generate SFT (overnight stay) files
- **`generate_and_upload_gp_data()`** - Generate GP (registered patient) files
- **`generate_and_upload_all_test_data()`** - Generate both with configurable overlap


## Required Environment Variables

Create a `.env` file in the `dev_utils/` directory:

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

## Features

- ✅ Generates valid NHS numbers with correct Modulus 11 checksums
- ✅ Creates SHA256 checksums in `sha256sum` format
- ✅ Uploads to S3 with KMS encryption
- ✅ Configurable SFT/GP overlap ratio
- ✅ Automatic local file cleanup after upload
- ✅ Separate functions for SFT and GP data generation

