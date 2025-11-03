# Presigned URL Generator & cURL Command for S3 Uploads

This Python script generates a **presigned S3 URL** for uploading a file with **KMS server-side encryption**, and
produces a ready-to-use `curl` command for the upload.

It can take parameters either from **environment variables** or from **command-line arguments**. Command-line arguments
override environment variables if both are provided.

---

## Requirements

- Python 3.11+
- `boto3` library installed
- AWS credentials configured for a user authorized to access the target S3 bucket and KMS key.

---

## Environment Variables

Set these if you want the script to pick them up automatically:

| Variable     | Example Value                                      | Description                                     |
|--------------|----------------------------------------------------|-------------------------------------------------|
| `KMS_KEY`    | `arn:aws:kms:eu-west-2:123456789012:key/abcd-1234` | ARN of the KMS key for server-side encryption   |
| `FILE_PATH`  | `my-bucket/uploads/`                               | S3 bucket and prefix in format `bucket/prefix/` |
| `FILE_NAME`  | `file.csv`                                         | Name of the file to upload                      |
| `EXPIRATION` | `3600`                                             | Expiration time of the presigned URL in seconds |

---

## Command-Line Arguments

All arguments are **optional**. If not provided, the script will fall back to environment variables.

| Argument       | 
|----------------|
| `--kms-key`    | 
| `--file-path`  | 
| `--file-name`  |
| `--expiration` | 

**Example usage:**

```bash
python generate_presigned_url.py \
    --kms-key arn:aws:kms:eu-west-2:123456789012:key/abcd-1234 \
    --file-path my-bucket/uploads/ \
    --file-name sft.csv \
    --expiration 3600
