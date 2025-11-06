import json
import logging
from typing import List

import boto3
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger()

s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')


def list_s3_files(bucket: str, prefix: str) -> List[str]:
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        files = []
        for page in page_iterator:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key != prefix and not key.endswith('/') and key.endswith('.csv'):
                    files.append(key)

        logger.info(f"Found {len(files)} CSV files in s3://{bucket}/{prefix}")
        return files

    except ClientError as e:
        logger.error(f"Failed to list files in s3://{bucket}/{prefix}: {e}", exc_info=True)
        raise


def read_s3_file(bucket: str, key: str) -> bytes:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        logger.info(f"Successfully read file s3://{bucket}/{key}")
        return content

    except ClientError as e:
        logger.error(f"Failed to read file s3://{bucket}/{key}: {e}", exc_info=True)
        raise


def write_to_s3(bucket: str, key: str, content: str, kms_key_id: str) -> None:
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode('utf-8'),
            ServerSideEncryption= 'aws:kms',
            SSEKMSKeyId=kms_key_id
        )
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logging.error(f'AWS error writing to s3://{bucket}/{key}: [{error_code}] {error_msg}')
        raise
    except (BotoCoreError, Exception) as e:
        logging.error(f'Failed to write to s3://{bucket}/{key}: {e}')
        raise


def delete_s3_file(bucket: str, key: str) -> None:
    try:
        logging.info(f'Deleting s3://{bucket}/{key}')
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Successfully deleted file s3://{bucket}/{key}")

    except ClientError as e:
        logger.error(f"Failed to delete file s3://{bucket}/{key}: {e}", exc_info=True)
        raise

def invoke_pseudonymisation_lambda_batch(
    field_name: str,
    field_values: List[str],
    function_name: str
) -> List[str]:
    if not field_name or not field_values:
        logger.warning(f"Field name or values list is empty, cannot pseudonymise")
        return []

    try:
        logger.debug(f"Pseudonymising batch of {len(field_values)} values for field: {field_name}")

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'action': 'encrypt',
                'field_name': field_name,
                'field_value': field_values
            })
        )

        result = json.loads(response['Payload'].read())

        if 'error' in result:
            error_msg = f"Pseudonymisation Lambda returned error: {result['error']}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if 'field_value' not in result:
            error_msg = "Pseudonymisation Lambda response missing 'field_value'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        pseudonymised_values = result['field_value']

        if not isinstance(pseudonymised_values, list):
            logger.error(f"Expected list response, got {type(pseudonymised_values)}")
            raise ValueError(f"Expected list response from pseudonymisation service")

        logger.debug(f"Successfully pseudonymised batch of {len(pseudonymised_values)} values for field: {field_name}")

        return pseudonymised_values

    except ValueError:
        raise
    except ClientError as e:
        logger.error(f"AWS ClientError while pseudonymising field '{field_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Exception occurred while pseudonymising field '{field_name}': {e}", exc_info=True)
        raise
