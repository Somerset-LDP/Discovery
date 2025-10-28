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

from lambda_functions.pseudonymisation.logging_utils import CorrelationLogger, JsonFormatter

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
    key_versions: Dict[str, str]
    current_version: str
    algorithm: str
    cache_ttl_hours: int


def load_config(log: CorrelationLogger) -> Config:
    log.info("Loading configuration from environment variables")
    kms_key_secret = os.getenv(ENV_SECRET_NAME_KMS_KEY)
    key_versions_secret = os.getenv(ENV_SECRET_NAME_KEY_VERSIONS)
    algorithm = os.getenv(ENV_ALGORITHM_ID)
    cache_ttl_hours = os.getenv(ENV_CACHE_TTL_HOURS, DEFAULT_CACHE_TTL_HOURS)

    kms_key_id = get_secret(kms_key_secret, log)
    key_versions_json = get_secret(key_versions_secret, log)
    key_versions_data = json.loads(key_versions_json)
    current_version = key_versions_data.get('current')
    key_versions = key_versions_data.get('keys', {})

    if not current_version:
        raise ValueError("Encrypted keys secret missing 'current' field")
    if not key_versions:
        raise ValueError("Encrypted keys secret missing 'keys' field")
    if current_version not in key_versions:
        raise ValueError(f"Current version '{current_version}' not found in keys")

    return Config(
        kms_key_id=kms_key_id,
        key_versions=key_versions,
        current_version=current_version,
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

    return secret_str


def get_data_key(key_version: str, config: Config, log: CorrelationLogger) -> bytes:
    global key_cache

    if key_version in key_cache:
        key_material, timestamp = key_cache[key_version]
        if datetime.now(UTC) - timestamp < timedelta(hours=config.cache_ttl_hours):
            log.info(f"Using cached data key for version '{key_version}'")
            return key_material
        log.info(f"Cached data key for version '{key_version}' expired, fetching new one")
        del key_cache[key_version]

    if key_version not in config.key_versions:
        raise ValueError(f"Key version '{key_version}' not found in configuration")

    encrypted_key_b64 = config.key_versions[key_version]
    encrypted_key_blob = base64.b64decode(encrypted_key_b64)

    log.info(f"Decrypting data key for version '{key_version}' using KMS")

    response = kms_client.decrypt(
        CiphertextBlob=encrypted_key_blob,
        KeyId=config.kms_key_id
    )
    key_material = response['Plaintext']

    key_cache[key_version] = (key_material, datetime.now(UTC))
    log.info(f"Data key for version '{key_version}' decrypted and cached")

    return key_material


def build_aad(field_name: str, key_version: str, config: Config) -> bytes:
    aad = AdditionalAuthenticatedData(
        field=field_name,
        algorithm=config.algorithm,
        key_version=key_version
    )
    return aad.to_bytes()


def encrypt_value(
    value: str,
    field_name: str,
    config: Config,
    log: CorrelationLogger
) -> str:
    if not value or not value.strip():
        raise ValueError("Value cannot be empty")

    key_version = config.current_version
    data_key = get_data_key(key_version, config, log)
    cipher = AESSIV(data_key)

    aad = build_aad(field_name, key_version, config)
    ciphertext = cipher.encrypt(value.encode(ENCODING_UTF8), [aad])
    return base64.urlsafe_b64encode(ciphertext).decode(ENCODING_ASCII)


def decrypt_value(
    pseudonym: str,
    field_name: str,
    config: Config,
    log: CorrelationLogger
) -> str:
    if not pseudonym or not pseudonym.strip():
        raise ValueError("Pseudonym cannot be empty")

    ciphertext = base64.urlsafe_b64decode(pseudonym)

    versions_to_try = [config.current_version] + [
        v for v in config.key_versions.keys() if v != config.current_version
    ]

    last_error = None
    for key_version in versions_to_try:
        try:
            data_key = get_data_key(key_version, config, log)
            cipher = AESSIV(data_key)
            aad = build_aad(field_name, key_version, config)
            plaintext = cipher.decrypt(ciphertext, [aad])
            log.info(f"Successfully decrypted using key version '{key_version}'")
            return plaintext.decode(ENCODING_UTF8)
        except Exception as e:
            last_error = e
            log.debug(f"Failed to decrypt with version '{key_version}': {str(e)}")
            continue

    raise ValueError(f"Failed to decrypt with any available key version. Last error: {str(last_error)}")


def process_field_encryption(
    field_name: str,
    field_value: Union[str, List[str]],
    config: Config,
    log: CorrelationLogger
) -> Union[str, List[str]]:
    if isinstance(field_value, list):
        if not field_value:
            log.warning(f"Empty list provided for field '{field_name}', skipping encryption")
            raise ValueError(f"Field '{field_name}' contains an empty list - cannot process")
        return [encrypt_value(v, field_name, config, log) for v in field_value]
    return encrypt_value(field_value, field_name, config, log)


def process_field_decryption(
    field_name: str,
    field_value: Union[str, List[str]],
    config: Config,
    log: CorrelationLogger
) -> Union[str, List[str]]:
    if isinstance(field_value, list):
        return [decrypt_value(v, field_name, config, log) for v in field_value]
    return decrypt_value(field_value, field_name, config, log)


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

        if action == 'encrypt':
            result = process_field_encryption(field_name, field_value, config, log)
            log.info("Encryption successful", extra={'field_name': field_name})
            response = PseudonymisationResponse(field_name=field_name, field_value=result)
            return response.to_dict()
        elif action == 'reidentify':
            result = process_field_decryption(field_name, field_value, config, log)
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
