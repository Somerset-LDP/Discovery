import logging
import os
from io import BytesIO
from typing import Any

import boto3
from botocore.exceptions import ClientError

from location.aws_lambda.layers.common.common import parse_to_datetime
from location.aws_lambda.layers.common.common_utils import (
    DataIngestionException,
    CHUNK_SIZE_BYTES
)

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("log_level", "DEBUG"))

s3_client = boto3.client("s3")


def create_s3_key(data_source: str, ingestion_timestamp: str, file_name: str) -> str:
    if not data_source or not data_source.strip():
        raise DataIngestionException("Cannot create s3 key: data_source is null or empty")
    if not file_name or not file_name.strip():
        raise DataIngestionException("Cannot create s3 key: file_name is null or empty")
    if not ingestion_timestamp or not ingestion_timestamp.strip():
        raise DataIngestionException("Cannot create s3 key: ingestion_timestamp is null or empty")

    s3_key_template = "landing/reference/{data_source}/{year}/{month}/{day}/{file_name}"
    dt = parse_to_datetime(ingestion_timestamp)
    s3_key = s3_key_template.format(
        data_source=data_source, year=f"{dt:%Y}", month=f"{dt:%m}", day=f"{dt:%d}", file_name=file_name
    )
    logger.debug(f"{data_source} S3 key: {s3_key}")
    return s3_key


def upload_to_s3(s3_bucket: str, s3_key: str, content: Any) -> None:
    if not s3_bucket or not s3_bucket.strip():
        raise DataIngestionException("S3 bucket name is null or empty")
    if not s3_key or not s3_key.strip():
        raise DataIngestionException("S3 key is null or empty")
    if not content:
        raise DataIngestionException("Content is null or empty")

    logger.debug(f"Uploading file to s3://{s3_bucket}/{s3_key}")
    try:
        if isinstance(content, BytesIO):
            content.seek(0)
            body = content.read()
        elif isinstance(content, bytes):
            body = content
        else:
            body = bytes(content)

        if not body:
            raise DataIngestionException("Content body is empty after conversion")

        logger.debug(f"Uploading {len(body)} bytes")
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=body
        )
        logger.info(f"File uploaded successfully to s3://{s3_bucket}/{s3_key}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        raise DataIngestionException(
            f"S3 upload failed to s3://{s3_bucket}/{s3_key}. Error: {error_code} - {error_msg}"
        )
    except DataIngestionException:
        raise
    except Exception as e:
        raise DataIngestionException(
            f"Unexpected error during upload to s3://{s3_bucket}/{s3_key}: {str(e)}"
        )


def upload_to_s3_multipart(s3_bucket: str, s3_key: str, file_stream: BytesIO) -> None:
    if not s3_bucket or not s3_bucket.strip():
        raise DataIngestionException("S3 bucket name is null or empty")
    if not s3_key or not s3_key.strip():
        raise DataIngestionException("S3 key is null or empty")
    if not file_stream:
        raise DataIngestionException("File stream is null or empty")

    logger.debug(f"Starting multipart upload to s3://{s3_bucket}/{s3_key}")

    try:
        file_stream.seek(0)

        multipart_upload = s3_client.create_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key
        )
        upload_id = multipart_upload['UploadId']

        parts = []
        part_number = 1
        total_bytes = 0

        try:
            while True:
                chunk = file_stream.read(CHUNK_SIZE_BYTES)
                if not chunk:
                    break

                logger.debug(f"Uploading part {part_number}, size: {len(chunk)} bytes")

                response = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )

                parts.append({
                    'PartNumber': part_number,
                    'ETag': response['ETag']
                })

                total_bytes += len(chunk)
                part_number += 1

            if not parts:
                s3_client.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id
                )
                raise DataIngestionException("No data to upload - file stream is empty")

            s3_client.complete_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )

            logger.info(
                f"Multipart upload completed: {total_bytes} bytes in {len(parts)} parts to s3://{s3_bucket}/{s3_key}")

        except DataIngestionException:
            raise
        except Exception as e:
            logger.error(f"Multipart upload failed, aborting: {str(e)}")
            s3_client.abort_multipart_upload(
                Bucket=s3_bucket,
                Key=s3_key,
                UploadId=upload_id
            )
            raise

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        raise DataIngestionException(
            f"S3 multipart upload failed to s3://{s3_bucket}/{s3_key}. Error: {error_code} - {error_msg}"
        )
    except DataIngestionException:
        raise
    except Exception as e:
        raise DataIngestionException(
            f"Unexpected error during multipart upload to s3://{s3_bucket}/{s3_key}: {str(e)}"
        )
