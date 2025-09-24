import os

import boto3



def generate_s3_presigned_url(bucket: str, key: str, kms_key: str, expiration: int = 3600) -> str:
    s3 = boto3.client('s3')
    url = s3.generate_presigned_url(
        'put_object',
        Params={
            'Bucket': bucket,
            'Key': key,
            'ServerSideEncryption': 'aws:kms',
            'SSEKMSKeyId': kms_key
        },
        ExpiresIn=expiration
    )
    print(f"\nGenerated S3 presigned URL: \n{url}")
    return url

def generate_curl_to_upload() -> str:
    kms_key = os.getenv("KMS_KEY_ID")
    sft_bucket = os.getenv("S3_SFT_FILE_PREFIX", "").split("/")[0]
    sft_prefix = os.getenv("S3_SFT_FILE_PREFIX", "").split("/", 1)[1]
    sft_key = os.path.join(sft_prefix, "sft.csv")
    expiration = 3600  # URL expiration time in seconds
    presigned_url = generate_s3_presigned_url(sft_bucket, sft_key, kms_key, expiration)

    curl_command = f"""
    curl.exe -X PUT "{presigned_url}" `
    -H "X-Amz-Server-Side-Encryption: aws:kms" `
    -H "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: {kms_key}" `
    --data-binary "<local_file_path>"
    """
    print(f"\nGenerated curl command:\n{curl_command}")
    return curl_command

if __name__ == "__main__":
    generate_curl_to_upload()