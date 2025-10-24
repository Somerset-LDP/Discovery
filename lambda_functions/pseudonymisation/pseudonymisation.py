import base64
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, UTC
from typing import Dict, Any, Tuple, Union, List

import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives.ciphers.aead import AESSIV
from dotenv import load_dotenv

from logging_utils import CorrelationLogger, JsonFormatter

load_dotenv()


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    else:
        for handler in logger.handlers:
            handler.setFormatter(JsonFormatter())

    return logger


logger = setup_logging()

ENV_SECRET_NAME_KMS_KEY = 'SECRET_NAME_KMS_KEY'
ENV_SECRET_NAME_KEY_VERSIONS = 'SECRET_NAME_KEY_VERSIONS'
ENV_ALGORITHM_ID = 'ALGORITHM_ID'
ENV_CACHE_TTL_HOURS = 'CACHE_TTL_HOURS'

ENCODING_UTF8 = 'utf-8'
ENCODING_ASCII = 'ascii'
KMS_KEY_SPEC_AES_256 = 'AES_256'
DEFAULT_CACHE_TTL_HOURS = 1

key_cache: Dict[str, Tuple[bytes, datetime]] = {}

kms_client = boto3.client('kms')
secrets_client = boto3.client('secretsmanager')


@dataclass
class AdditionalAuthenticatedData:
    field: str
    algorithm: str
    key_version: str

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True).encode(ENCODING_UTF8)


@dataclass
class PseudonymisationResponse:
    field_name: str
    field_value: Union[str, List[str]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ErrorResponse:
    error: str
    correlation_id: str = None

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class Config:
    kms_key_id: str
    key_versions: str
    algorithm: str
    cache_ttl_hours: int


def load_config(log: CorrelationLogger) -> Config:
    log.info("Loading configuration from environment variables")
    kms_key_secret = os.getenv(ENV_SECRET_NAME_KMS_KEY)
    key_versions_secret = os.getenv(ENV_SECRET_NAME_KEY_VERSIONS)
    algorithm_secret = os.getenv(ENV_ALGORITHM_ID)
    cache_ttl_hours = os.getenv(ENV_CACHE_TTL_HOURS, DEFAULT_CACHE_TTL_HOURS)

    kms_key_id = get_secret(kms_key_secret, log)
    key_versions = get_secret(key_versions_secret, log)
    algorithm = algorithm_secret

    return Config(
        kms_key_id=kms_key_id,
        key_versions=key_versions,
        algorithm=algorithm,
        cache_ttl_hours=int(cache_ttl_hours)
    )


def get_secret(secret_name: str, log: CorrelationLogger) -> str:
    log.info(f"Retrieving secret: {secret_name}")
    if not secret_name:
        msg = "Secret name cannot be empty"
        log.error(msg)
        raise ValueError(msg)

    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']

    if not secret_str or not secret_str.strip():
        msg = f"Secret '{secret_name}' is empty or contains only whitespace"
        log.error(msg)
        raise ValueError(msg)

    if secret_str.startswith('{'):
        secret_json = json.loads(secret_str)
        if isinstance(secret_json, dict) and secret_json:
            value = next(iter(secret_json.values()))
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            return value

    return secret_str


def get_data_key(kms_key_id: str, config: Config, log: CorrelationLogger) -> bytes:
    global key_cache

    if kms_key_id in key_cache:
        key_material, timestamp = key_cache[kms_key_id]
        if datetime.now(UTC) - timestamp < timedelta(hours=config.cache_ttl_hours):
            log.info("Using cached data key")
            return key_material
        log.info("Cached data key expired, generating new one")
        del key_cache[kms_key_id]

    log.info("Generating new data key from KMS")
    response = kms_client.generate_data_key(KeyId=kms_key_id, KeySpec=KMS_KEY_SPEC_AES_256)
    key_material = response['Plaintext']
    key_cache[kms_key_id] = (key_material, datetime.now(UTC))
    log.info("Data key generated and cached")
    return key_material


def build_aad(field_name: str, config: Config, log: CorrelationLogger) -> bytes:
    key_versions = json.loads(config.key_versions)
    current_version = key_versions.get('current')
    if not current_version:
        raise ValueError("Key versions secret missing 'current' field")

    aad = AdditionalAuthenticatedData(
        field=field_name,
        algorithm=config.algorithm,
        key_version=current_version
    )
    return aad.to_bytes()


def encrypt_value(
    value: str,
    field_name: str,
    cipher: AESSIV,
    config: Config,
    log: CorrelationLogger
) -> str:
    if not value or not value.strip():
        raise ValueError("Value cannot be empty")

    aad = build_aad(field_name, config, log)
    ciphertext = cipher.encrypt(value.encode(ENCODING_UTF8), [aad])
    return base64.urlsafe_b64encode(ciphertext).decode(ENCODING_ASCII)


def decrypt_value(
    pseudonym: str,
    field_name: str,
    cipher: AESSIV,
    config: Config,
    log: CorrelationLogger
) -> str:
    if not pseudonym or not pseudonym.strip():
        raise ValueError("Pseudonym cannot be empty")

    aad = build_aad(field_name, config, log)
    ciphertext = base64.urlsafe_b64decode(pseudonym)
    plaintext = cipher.decrypt(ciphertext, [aad])
    return plaintext.decode(ENCODING_UTF8)


def process_field_encryption(
    field_name: str,
    field_value: Union[str, List[str]],
    cipher: AESSIV,
    config: Config,
    log: CorrelationLogger
) -> Union[str, List[str]]:
    if isinstance(field_value, list):
        if not field_value:
            log.warning(f"Empty list provided for field '{field_name}', skipping encryption")
            raise ValueError(f"Field '{field_name}' contains an empty list - cannot process")
        return [encrypt_value(v, field_name, cipher, config, log) for v in field_value]
    return encrypt_value(field_value, field_name, cipher, config, log)


def process_field_decryption(
    field_name: str,
    field_value: Union[str, List[str]],
    cipher: AESSIV,
    config: Config,
    log: CorrelationLogger
) -> Union[str, List[str]]:
    if isinstance(field_value, list):
        return [decrypt_value(v, field_name, cipher, config, log) for v in field_value]
    return decrypt_value(field_value, field_name, cipher, config, log)


def validate_env_vars(log: CorrelationLogger):
    required_vars = {
        'SECRET_NAME_KMS_KEY': ENV_SECRET_NAME_KMS_KEY,
        'SECRET_NAME_KEY_VERSIONS': ENV_SECRET_NAME_KEY_VERSIONS,
        'ALGORITHM_ID': ENV_ALGORITHM_ID
    }

    missing_vars = []
    for var_name, env_key in required_vars.items():
        if not os.getenv(env_key):
            missing_vars.append(var_name)

    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        log.error(error_msg)
        raise ValueError(error_msg)


def validate_event(event: Dict[str, Any], log: CorrelationLogger) -> None:
    required_fields = ['action', 'field_name', 'field_value']

    missing_fields = []
    for field in required_fields:
        if field not in event or event[field] is None:
            missing_fields.append(field)

    if missing_fields:
        error_msg = f"Missing required event fields: {', '.join(missing_fields)}"
        log.error(error_msg)
        raise ValueError(error_msg)

    action = event['action']
    if action not in ['encrypt', 'reidentify']:
        error_msg = f"Invalid action: {action}. Must be 'encrypt' or 'reidentify'"
        log.error(error_msg)
        raise ValueError(error_msg)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    correlation_id = event.get('correlation_id')
    log = CorrelationLogger(logger, correlation_id)

    try:
        validate_event(event, log)
        validate_env_vars(log)

        action = event['action']
        field_name = event['field_name']
        field_value = event['field_value']

        log.info("Request received", extra={'action': action, 'field_name': field_name})

        config = load_config(log)
        data_key = get_data_key(config.kms_key_id, config, log)
        cipher = AESSIV(data_key)

        if action == 'encrypt':
            result = process_field_encryption(field_name, field_value, cipher, config, log)
            log.info("Encryption successful", extra={'field_name': field_name})
            response = PseudonymisationResponse(field_name=field_name, field_value=result)
            return response.to_dict()
        elif action == 'reidentify':
            result = process_field_decryption(field_name, field_value, cipher, config, log)
            log.info("Reidentification successful", extra={'field_name': field_name})
            response = PseudonymisationResponse(field_name=field_name, field_value=result)
            return response.to_dict()

    except ValueError as e:
        log.error(f"Validation error: {str(e)}")
        response = ErrorResponse(error=str(e), correlation_id=correlation_id)
        return response.to_dict()
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = f"AWS service error: {error_code}"
        log.error(f"AWS service error: {e}")
        response = ErrorResponse(error=error_msg, correlation_id=correlation_id)
        return response.to_dict()
    except Exception as e:
        log.error(f"Unexpected error: {str(e)}", exc_info=True)
        response = ErrorResponse(error=f"Encryption/decryption failed: {str(e)}", correlation_id=correlation_id)
        return response.to_dict()
