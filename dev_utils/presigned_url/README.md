# Presigned URL Utilities

This folder contains tools for generating **presigned S3 URLs** for uploading files with **KMS server-side encryption**.

---

## Files Overview

| File                          | Description                                                        |
|-------------------------------|--------------------------------------------------------------------|
| `presigned_url_utils.py`      | Core utility for generating presigned URLs and curl commands       |
| `gp_generate_cohort_curls.py` | Batch script for generating presigned URLs for GP cohort uploads   |
| `gp_cohort_upload.ps1`        | PowerShell script for uploading cohort files using presigned URLs  |
| `calc_hash.bat`               | Batch script to compute SHA256 hashes for CSV files in a directory |

---

## Requirements

- Python 3.11+
- `boto3` library installed
- AWS credentials configured for a user authorised to access the target S3 bucket and KMS key

---

## presigned_url_utils.py

Generates a presigned S3 URL and produces a ready-to-use `curl` command for uploading.

### Environment Variables

Set these in `.env` or export them in your shell:

| Variable     | Example Value                                      | Description                                     |
|--------------|----------------------------------------------------|-------------------------------------------------|
| `KMS_KEY`    | `arn:aws:kms:eu-west-2:123456789012:key/abcd-1234` | ARN of the KMS key for server-side encryption   |
| `FILE_PATH`  | `my-bucket/uploads/`                               | S3 bucket and prefix in format `bucket/prefix/` |
| `FILE_NAME`  | `file.csv`                                         | Name of the file to upload                      |
| `EXPIRATION` | `3600`                                             | Expiration time of the presigned URL in seconds |

### Command-Line Arguments

All arguments are **optional**. If not provided, the script falls back to environment variables.

| Argument       | Description                                      |
|----------------|--------------------------------------------------|
| `--kms-key`    | KMS Key ARN                                      |
| `--file-path`  | S3 bucket and prefix (e.g. `my-bucket/uploads/`) |
| `--file-name`  | Name of the file to upload                       |
| `--expiration` | Presigned URL expiration in seconds              |

### Example Usage

```bash
python presigned_url_utils.py \
    --kms-key arn:aws:kms:eu-west-2:123456789012:key/abcd-1234 \
    --file-path my-bucket/uploads/ \
    --file-name data.csv \
    --expiration 3600
```

---

## gp_generate_cohort_curls.py

Batch generates presigned URLs for uploading cohort CSV files for a list of GP practices.

### Usage

1. Configure environment variables in `.env`:
    - `KMS_KEY` - KMS key ARN
    - `FILE_PATH` - S3 bucket and prefix

2. Edit the list of GP ODS codes in the script

3. Run:
   ```bash
   python gp_generate_cohort_curls.py
   ```

---

## gp_cohort_upload.ps1

PowerShell script for uploading cohort files using presigned URLs.

### Parameters

| Parameter        | Required | Description                                    |
|------------------|----------|------------------------------------------------|
| `-InputFolder`   | Yes      | Path to folder containing files to upload      |
| `-OutputFolder`  | Yes      | Path to folder for output/logs                 |
| `-ConfigFile`    | Yes      | Path to configuration file with presigned URLs |
| `-SecurityToken` | Yes      | AWS security token                             |
| `-Credential`    | Yes      | AWS credential                                 |
| `-EncryptionKey` | Yes      | KMS encryption key ARN                         |
| `-DryRun`        | No       | Run without actually uploading                 |

---

## calc_hash.bat

Windows batch script to compute SHA256 hashes for all CSV files in a directory.

### Usage

```cmd
calc_hash.bat "C:\path\to\directory"
```

