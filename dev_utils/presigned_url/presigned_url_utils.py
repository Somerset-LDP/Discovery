import argparse
import os
import sys

import boto3
from botocore.exceptions import BotoCoreError, ClientError

s3_client = boto3.client("s3")


def generate_s3_presigned_url(bucket: str, key: str, kms_key: str, expiration: int) -> str:
    try:
        url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": kms_key,
            },
            ExpiresIn=expiration,
        )
        print(f"Generated S3 presigned URL for s3://{bucket}/{key}")
        return url
    except (BotoCoreError, ClientError) as e:
        print(f"Failed to generate presigned URL for s3://{bucket}/{key}: {e}")
        raise


def generate_curl_to_upload(kms_key: str, file_path: str, file_name: str, expiration: int) -> str:
    if "/" not in file_path:
        raise ValueError(f"Invalid file_path format: {file_path}. Expected '<bucket>/<prefix>'")

    bucket, prefix = file_path.split("/", 1)
    key = f"{prefix}{file_name}"

    presigned_url = generate_s3_presigned_url(bucket, key, kms_key, expiration)

    curl_command = (
        f'curl.exe -X PUT "{presigned_url}" ^\n'
        f'-H "X-Amz-Server-Side-Encryption: aws:kms" ^\n'
        f'-H "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: {kms_key}" ^\n'
        f'--data-binary "<local_file_path>"'
    )

    print(f"\nUse the following curl command to upload file:\n{curl_command}\n")
    return curl_command


def parse_cli_args():
    parser = argparse.ArgumentParser(description="Generate S3 presigned URL and curl command")
    parser.add_argument("--kms-key", help="KMS Key ARN")
    parser.add_argument("--file-path", help="S3 prefix (e.g. my-bucket/uploads)")
    parser.add_argument("--file-name", help="Name of the file to upload")
    parser.add_argument("--expiration", type=int, help="Presigned URL expiration in seconds")

    return parser.parse_args()


def get_params(args):
    kms_key = args.kms_key or os.getenv("KMS_KEY")
    file_path = args.file_path or os.getenv("FILE_PATH")
    file_name = args.file_name or os.getenv("FILE_NAME")
    expiration = args.expiration or os.getenv("EXPIRATION")
    return kms_key, file_path, file_name, expiration


def validate_params(kms_key, file_path, file_name, expiration):
    missing = [name for name, val in [
        ("KMS_KEY / --kms-key", kms_key),
        ("FILE_PATH / --file-path", file_path),
        ("FILE_NAME / --file-name", file_name),
        ("EXPIRATION / --expiration", expiration)
    ] if not val]

    if missing:
        print(f"Missing required parameter(s): {', '.join(missing)}")
        sys.exit(1)


def main():
    args = parse_cli_args()
    kms_key, file_path, file_name, expiration = get_params(args)
    validate_params(kms_key, file_path, file_name, expiration)
    generate_curl_to_upload(kms_key, file_path, file_name, expiration)


if __name__ == "__main__":
    main()
