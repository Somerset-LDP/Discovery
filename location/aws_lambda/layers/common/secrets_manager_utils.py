import logging
import os

import boto3
from botocore.exceptions import ClientError

from location.aws_lambda.layers.common.common_utils import DataIngestionException

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG"))


def get_secret_value(secret_name: str) -> str:
    if not secret_name:
        raise DataIngestionException("Secret name is required")

    logger.debug(f"Fetching secret: {secret_name}")

    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        return response["SecretString"]
    except ClientError as e:
        raise DataIngestionException(f"Failed to retrieve secret '{secret_name}': {e}")

