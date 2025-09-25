import os
import sys
import argparse
import boto3


def generate_s3_presigned_url(bucket: str, key: str, kms_key: str, expiration: int) -> str:
    s3 = boto3.client("s3")
    url = s3.generate_presigned_url(
        "put_object",
        Params={
            "Bucket": bucket,
            "Key": key,
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": kms_key,
        },
        ExpiresIn=expiration,
    )
    print(f"\nGenerated S3 presigned URL: \n{url}")
    return url


def generate_curl_to_upload(kms_key: str, sft_path: str, file_name: str, expiration: int) -> str:
    sft_bucket, sft_prefix = sft_path.split("/", 1)
    sft_key = os.path.join(sft_prefix, file_name)

    presigned_url = generate_s3_presigned_url(sft_bucket, sft_key, kms_key, expiration)

    # Works for powershell, bash, and postman
    curl_command = f"""
    curl.exe -X PUT "{presigned_url}" -H "X-Amz-Server-Side-Encryption: aws:kms" -H "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: {kms_key}" --data-binary "<local_file_path>"
    """
    print(f"\nGenerated curl command:\n{curl_command}")
    return curl_command


def main():
    parser = argparse.ArgumentParser(description="Generate S3 presigned URL and curl command")
    parser.add_argument("--kms-key", help="KMS Key ARN")
    parser.add_argument("--sft-path", help="S3 prefix (e.g. my-bucket/uploads)")
    parser.add_argument("--file-name", help="Name of the file to upload")
    parser.add_argument("--expiration", type=int, help="Presigned URL expiration in seconds")

    args = parser.parse_args()

    # CLI args > env vars
    kms_key = args.kms_key or os.getenv("KMS_KEY_ID")
    sft_path = args.sft_path or os.getenv("S3_SFT_FILE_PREFIX")
    file_name = args.file_name or os.getenv("FILE_NAME")
    expiration = args.expiration or os.getenv("PRESIGN_EXPIRATION")

    # Validation
    missing = []
    if not kms_key:
        missing.append("KMS_KEY_ID / --kms-key")
    if not sft_path:
        missing.append("S3_SFT_FILE_PREFIX / --sft-path")
    if not file_name:
        missing.append("FILE_NAME / --file-name")
    if not expiration:
        missing.append("PRESIGN_EXPIRATION / --expiration")

    if missing:
        print(f"Missing required parameter(s): {', '.join(missing)}")
        sys.exit(1)

    expiration = int(expiration)

    generate_curl_to_upload(kms_key, sft_path, file_name, expiration)


if __name__ == "__main__":
    main()
