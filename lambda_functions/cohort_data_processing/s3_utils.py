import logging
import csv
from io import StringIO
import boto3
from typing import List, Set


s3_client = boto3.client('s3')


def list_s3_files(bucket: str, prefix: str) -> List[str]:
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        keys = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key != prefix and not key.endswith("/"):
                    keys.append(key)
        return keys
    except Exception as e:
        logging.error(f"Failed to list files in s3://{bucket}/{prefix}: {e}")
        raise


def get_s3_object_content(bucket: str, key: str) -> bytes:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        logging.error(f'Error getting object s3://{bucket}/{key}: {e}')
        raise


def write_to_s3(bucket: str, key: str, nhs_set: Set[str]) -> None:
    try:
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        for nhs in sorted(nhs_set):
            writer.writerow([nhs])
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue().encode('utf-8'))
        logging.info(f'Written final union to s3://{bucket}/{key}')
    except Exception as e:
        logging.error(f'Failed to write to s3://{bucket}/{key}: {e}')
        raise


def delete_s3_objects(bucket: str, keys: List[str]) -> None:
    try:
        for key in keys:
            logging.info(f'Deleting s3://{bucket}/{key}')
            s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        logging.error(f'Failed to delete objects in {bucket}: {e}')
        raise
