import logging
import csv
from io import StringIO
import boto3


s3_client = boto3.client('s3')


def list_s3_files(bucket, prefix):
    try:
        logging.info(f"Listing files in S3 bucket, path: s3://{bucket}/{prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        # Remove the prefix itself (folder) and any folders
        filtered = [k for k in keys if k != prefix and not k.endswith('/')]
        return filtered
    except Exception as e:
        logging.error(f"Failed to list files in s3://{bucket}/{prefix}: {e}")
        raise


def get_s3_object_content(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        logging.error(f'Error getting object s3://{bucket}/{key}: {e}')
        raise


def write_to_s3(bucket, key, nhs_set):
    try:
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        for nhs in nhs_set:
            writer.writerow([nhs])
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))
        logging.info(f'Written final union to s3://{bucket}/{key}')
    except Exception as e:
        logging.error(f'Failed to write to s3://{bucket}/{key}: {e}')
        raise


def delete_s3_objects(bucket, keys):
    try:
        for key in keys:
            logging.info(f'Deleting s3://{bucket}/{key}')
            s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        logging.error(f'Failed to delete objects in {bucket}: {e}')
        raise
