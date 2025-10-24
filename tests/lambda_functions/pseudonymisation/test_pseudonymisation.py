import pytest
import os
from unittest.mock import patch

from botocore.exceptions import ClientError

from lambda_functions.pseudonymisation.pseudonymisation import (
    load_config,
    get_secret,
    Config,
    get_data_key,
    build_aad,
    encrypt_value,
    key_cache,
    decrypt_value,
    process_field_encryption,
    process_field_decryption,
    validate_env_vars,
    validate_event,
)
from lambda_functions.pseudonymisation.logging_utils import CorrelationLogger
import logging
from datetime import datetime, timedelta
from dateutil.tz import UTC
import json
import base64
from cryptography.hazmat.primitives.ciphers.aead import AESSIV


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_env():
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_logger():
    base_logger = logging.getLogger('test')
    return CorrelationLogger(base_logger, 'test-correlation-id')


@pytest.fixture
def mock_secrets_client():
    with patch('lambda_functions.pseudonymisation.pseudonymisation.secrets_client') as mock:
        yield mock


@pytest.fixture
def env_vars_set():
    env = {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }
    with patch.dict(os.environ, env, clear=False):
        yield env


@pytest.fixture
def mock_kms_client():
    with patch('lambda_functions.pseudonymisation.pseudonymisation.kms_client') as mock:
        yield mock


@pytest.fixture
def config():
    return Config(
        kms_key_id='arn:aws:kms:region:account:key/id',
        key_versions='{"current": "v1"}',
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )


@pytest.fixture
def cipher(config):
    key_material = b'0' * 32
    return AESSIV(key_material)


# ============================================================================
# load_config TESTS
# ============================================================================

@pytest.mark.parametrize('cache_ttl,expected', [
    ('2', 2),
    (None, 1),
])
def test_load_config_cache_ttl(mock_logger, env_vars_set, cache_ttl, expected):
    if cache_ttl:
        os.environ['CACHE_TTL_HOURS'] = cache_ttl

    with patch('lambda_functions.pseudonymisation.pseudonymisation.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = [
            'arn:aws:kms:region:account:key/id',
            '{"current": "v1"}'
        ]

        config = load_config(mock_logger)

        assert config.cache_ttl_hours == expected
        assert config.kms_key_id == 'arn:aws:kms:region:account:key/id'
        assert config.key_versions == '{"current": "v1"}'
        assert config.algorithm == 'AES-SIV'


def test_load_config_missing_kms_key_secret(mock_logger):
    with patch.dict(os.environ, {'ALGORITHM_ID': 'AES-SIV'}, clear=True):
        with pytest.raises(ValueError, match='Secret name cannot be empty'):
            load_config(mock_logger)


# ============================================================================
# get_secret TESTS
# ============================================================================

@pytest.mark.parametrize('secret_str,expected', [
    ('{"pseudonymisation/kms-key-id": "arn:aws:kms:region:account:key/id"}', 'arn:aws:kms:region:account:key/id'),
    ('plain-text-secret', 'plain-text-secret'),
    ('{"single": "value"}', 'value'),
    ('{"nested": {"key": "value"}}', '{"key": "value"}'),
])
def test_get_secret_success(mock_logger, mock_secrets_client, secret_str, expected):
    mock_secrets_client.get_secret_value.return_value = {'SecretString': secret_str}

    result = get_secret('test-secret', mock_logger)

    assert result == expected


@pytest.mark.parametrize('secret_str,error_msg', [
    ('   ', 'empty or contains only whitespace'),
    ('', 'empty or contains only whitespace'),
])
def test_get_secret_empty_value(mock_logger, mock_secrets_client, secret_str, error_msg):
    mock_secrets_client.get_secret_value.return_value = {'SecretString': secret_str}

    with pytest.raises(ValueError, match=error_msg):
        get_secret('test-secret', mock_logger)


def test_get_secret_empty_name(mock_logger):
    with pytest.raises(ValueError, match='Secret name cannot be empty'):
        get_secret('', mock_logger)


def test_get_secret_json_with_nested_object(mock_logger, mock_secrets_client):
    secret_json = '{"config": {"current": "v1", "previous": ["v0"]}}'
    mock_secrets_client.get_secret_value.return_value = {'SecretString': secret_json}

    result = get_secret('test-secret', mock_logger)

    assert result == '{"current": "v1", "previous": ["v0"]}'
    assert isinstance(result, str)


def test_get_secret_aws_error(mock_logger, mock_secrets_client):
    error_response = {'Error': {'Code': 'ResourceNotFoundException'}}
    mock_secrets_client.get_secret_value.side_effect = ClientError(error_response, 'GetSecretValue')

    with pytest.raises(ClientError):
        get_secret('nonexistent-secret', mock_logger)


# ============================================================================
# get_data_key TESTS
# ============================================================================

@pytest.mark.parametrize('ttl_hours,should_use_cache', [
    (1, True),
    (24, True),
])
def test_get_data_key_uses_cache(mock_logger, mock_kms_client, config, ttl_hours, should_use_cache):
    config.cache_ttl_hours = ttl_hours

    key_material = b'test_key_material_32_bytes_long!'
    key_cache['test-key-id'] = (key_material, datetime.now(UTC))

    result = get_data_key('test-key-id', config, mock_logger)

    assert result == key_material
    mock_kms_client.generate_data_key.assert_not_called()


def test_get_data_key_generates_new_when_expired(mock_logger, mock_kms_client, config):
    key_material = b'test_key_material_32_bytes_long!'
    old_timestamp = datetime.now(UTC) - timedelta(hours=2)
    key_cache['test-key-id'] = (b'old_key', old_timestamp)

    mock_kms_client.generate_data_key.return_value = {'Plaintext': key_material}

    result = get_data_key('test-key-id', config, mock_logger)

    assert result == key_material
    mock_kms_client.generate_data_key.assert_called_once()


def test_get_data_key_generates_new_on_cache_miss(mock_logger, mock_kms_client, config):
    key_cache.clear()
    key_material = b'test_key_material_32_bytes_long!'
    mock_kms_client.generate_data_key.return_value = {'Plaintext': key_material}

    result = get_data_key('new-key-id', config, mock_logger)

    assert result == key_material
    mock_kms_client.generate_data_key.assert_called_once_with(
        KeyId='new-key-id',
        KeySpec='AES_256'
    )


def test_get_data_key_caches_result(mock_logger, mock_kms_client, config):
    key_cache.clear()
    key_material = b'test_key_material_32_bytes_long!'
    mock_kms_client.generate_data_key.return_value = {'Plaintext': key_material}

    get_data_key('cache-test-key', config, mock_logger)

    assert 'cache-test-key' in key_cache
    cached_material, timestamp = key_cache['cache-test-key']
    assert cached_material == key_material
    assert isinstance(timestamp, datetime)


def test_get_data_key_kms_error(mock_logger, mock_kms_client, config):
    key_cache.clear()
    error_response = {'Error': {'Code': 'InvalidKeyId.NotFound'}}
    mock_kms_client.generate_data_key.side_effect = ClientError(error_response, 'GenerateDataKey')

    with pytest.raises(ClientError):
        get_data_key('invalid-key', config, mock_logger)


# ============================================================================
# build_aad TESTS
# ============================================================================

@pytest.mark.parametrize('field_name,algorithm,key_version', [
    ('nhs_number', 'AES-SIV', 'v1'),
    ('date_of_birth', 'AES-SIV', 'v2'),
    ('postcode', 'AES-GCM-SIV', 'v1'),
])
def test_build_aad_success(mock_logger, field_name, algorithm, key_version):
    key_versions_json = json.dumps({'current': key_version})
    config = Config(
        kms_key_id='test-key',
        key_versions=key_versions_json,
        algorithm=algorithm,
        cache_ttl_hours=1
    )

    result = build_aad(field_name, config, mock_logger)

    assert isinstance(result, bytes)
    aad_dict = json.loads(result.decode('utf-8'))
    assert aad_dict['field'] == field_name
    assert aad_dict['algorithm'] == algorithm
    assert aad_dict['key_version'] == key_version


def test_build_aad_missing_current_version(mock_logger):
    config = Config(
        kms_key_id='test-key',
        key_versions=json.dumps({'previous': 'v0'}),
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )

    with pytest.raises(ValueError, match="Key versions secret missing 'current' field"):
        build_aad('test_field', config, mock_logger)


@pytest.mark.parametrize('field_name', [
    'simple_field',
    'field_with_underscore',
    'UPPERCASE_FIELD',
    'field123',
])
def test_build_aad_various_field_names(mock_logger, field_name):
    config = Config(
        kms_key_id='test-key',
        key_versions=json.dumps({'current': 'v1'}),
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )

    result = build_aad(field_name, config, mock_logger)

    aad_dict = json.loads(result.decode('utf-8'))
    assert aad_dict['field'] == field_name


def test_build_aad_json_structure(mock_logger):
    config = Config(
        kms_key_id='test-key',
        key_versions=json.dumps({'current': 'v1'}),
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )

    result = build_aad('test_field', config, mock_logger)

    aad_dict = json.loads(result.decode('utf-8'))
    expected_keys = {'field', 'algorithm', 'key_version'}
    assert set(aad_dict.keys()) == expected_keys


def test_build_aad_sorted_keys(mock_logger):
    config = Config(
        kms_key_id='test-key',
        key_versions=json.dumps({'current': 'v1'}),
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )

    result = build_aad('test_field', config, mock_logger)
    result_str = result.decode('utf-8')

    expected = json.dumps(
        {'algorithm': 'AES-SIV', 'field': 'test_field', 'key_version': 'v1'},
        sort_keys=True
    )
    assert result_str == expected


# ============================================================================
# encrypt_value TESTS
# ============================================================================

@pytest.mark.parametrize('value,field_name', [
    ('NHS123456789', 'nhs_number'),
    ('1990-01-15', 'date_of_birth'),
    ('SW1A 1AA', 'postcode'),
    ('john.doe@example.com', 'email'),
])
def test_encrypt_value_success(mock_logger, config, cipher, value, field_name):
    result = encrypt_value(value, field_name, cipher, config, mock_logger)

    assert isinstance(result, str)
    assert len(result) > 0
    assert result != value


def test_encrypt_value_returns_base64(mock_logger, config, cipher):
    result = encrypt_value('test_value', 'field', cipher, config, mock_logger)

    decoded = base64.urlsafe_b64decode(result)
    assert isinstance(decoded, bytes)
    assert len(decoded) > 0


def test_encrypt_value_empty_value_raises_error(mock_logger, config, cipher):
    with pytest.raises(ValueError, match="Value cannot be empty"):
        encrypt_value('', 'field', cipher, config, mock_logger)


def test_encrypt_value_whitespace_value_raises_error(mock_logger, config, cipher):
    with pytest.raises(ValueError, match="Value cannot be empty"):
        encrypt_value('   ', 'field', cipher, config, mock_logger)


def test_encrypt_value_deterministic(mock_logger, config, cipher):
    value = 'test_value'
    field_name = 'test_field'

    result1 = encrypt_value(value, field_name, cipher, config, mock_logger)
    result2 = encrypt_value(value, field_name, cipher, config, mock_logger)

    assert result1 == result2


def test_encrypt_value_different_fields_different_output(mock_logger, config, cipher):
    value = 'same_value'

    result1 = encrypt_value(value, 'field1', cipher, config, mock_logger)
    result2 = encrypt_value(value, 'field2', cipher, config, mock_logger)

    assert result1 != result2


def test_encrypt_value_different_values_different_output(mock_logger, config, cipher):
    field = 'test_field'

    result1 = encrypt_value('value1', field, cipher, config, mock_logger)
    result2 = encrypt_value('value2', field, cipher, config, mock_logger)

    assert result1 != result2


@pytest.mark.parametrize('value', [
    'a',
    'single value',
    'value with spaces and numbers 123',
    'special!@#$%^&*()',
    'unicode_café_naïve',
])
def test_encrypt_value_various_inputs(mock_logger, config, cipher, value):
    result = encrypt_value(value, 'field', cipher, config, mock_logger)

    assert isinstance(result, str)
    assert len(result) > 0
    assert result != value


# ============================================================================
# decrypt_value TESTS
# ============================================================================

@pytest.mark.parametrize('field_name', [
    'nhs_number',
    'date_of_birth',
    'postcode',
    'email',
])
def test_decrypt_value_success(mock_logger, config, cipher, field_name):
    plaintext = 'test_value_123'
    encrypted = encrypt_value(plaintext, field_name, cipher, config, mock_logger)

    result = decrypt_value(encrypted, field_name, cipher, config, mock_logger)

    assert result == plaintext


def test_decrypt_value_empty_pseudonym_raises_error(mock_logger, config, cipher):
    with pytest.raises(ValueError, match="Pseudonym cannot be empty"):
        decrypt_value('', 'field', cipher, config, mock_logger)


def test_decrypt_value_whitespace_pseudonym_raises_error(mock_logger, config, cipher):
    with pytest.raises(ValueError, match="Pseudonym cannot be empty"):
        decrypt_value('   ', 'field', cipher, config, mock_logger)


def test_decrypt_value_invalid_base64_raises_error(mock_logger, config, cipher):
    with pytest.raises(Exception):
        decrypt_value('not_valid_base64!!!', 'field', cipher, config, mock_logger)


def test_decrypt_value_wrong_field_name_raises_error(mock_logger, config, cipher):
    plaintext = 'test_value'
    encrypted = encrypt_value(plaintext, 'field1', cipher, config, mock_logger)

    with pytest.raises(Exception):
        decrypt_value(encrypted, 'field2', cipher, config, mock_logger)


def test_decrypt_value_wrong_key_raises_error(mock_logger, config, cipher):
    plaintext = 'test_value'
    encrypted = encrypt_value(plaintext, 'field', cipher, config, mock_logger)

    wrong_key_material = b'1' * 32
    wrong_cipher = AESSIV(wrong_key_material)

    with pytest.raises(Exception):
        decrypt_value(encrypted, 'field', wrong_cipher, config, mock_logger)


@pytest.mark.parametrize('plaintext', [
    'NHS123456789',
    '1990-01-15',
    'SW1A 1AA',
    'john.doe@example.com',
    'special!@#$%^&*()',
    'unicode_café_naïve',
    'a',
    'value with spaces',
])
def test_decrypt_value_various_inputs(mock_logger, config, cipher, plaintext):
    encrypted = encrypt_value(plaintext, 'field', cipher, config, mock_logger)

    result = decrypt_value(encrypted, 'field', cipher, config, mock_logger)

    assert result == plaintext


def test_decrypt_value_roundtrip_deterministic(mock_logger, config, cipher):
    plaintext = 'test_value'
    field_name = 'test_field'

    encrypted1 = encrypt_value(plaintext, field_name, cipher, config, mock_logger)
    decrypted1 = decrypt_value(encrypted1, field_name, cipher, config, mock_logger)

    encrypted2 = encrypt_value(plaintext, field_name, cipher, config, mock_logger)
    decrypted2 = decrypt_value(encrypted2, field_name, cipher, config, mock_logger)

    assert decrypted1 == decrypted2 == plaintext


def test_decrypt_value_returns_string(mock_logger, config, cipher):
    plaintext = 'test_value'
    encrypted = encrypt_value(plaintext, 'field', cipher, config, mock_logger)

    result = decrypt_value(encrypted, 'field', cipher, config, mock_logger)

    assert isinstance(result, str)


# ============================================================================
# process_field_encryption TESTS
# ============================================================================

def test_process_field_encryption_single_value(mock_logger, config, cipher):
    field_name = 'nhs_number'
    value = 'NHS123456789'

    result = process_field_encryption(field_name, value, cipher, config, mock_logger)

    assert isinstance(result, str)
    assert result != value


def test_process_field_encryption_list_of_values(mock_logger, config, cipher):
    field_name = 'emails'
    values = ['test1@example.com', 'test2@example.com', 'test3@example.com']

    result = process_field_encryption(field_name, values, cipher, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == 3
    for encrypted, original in zip(result, values):
        assert encrypted != original
        assert isinstance(encrypted, str)


def test_process_field_encryption_empty_list_raises_error(mock_logger, config, cipher):
    field_name = 'test_field'
    values = []

    with pytest.raises(ValueError, match="contains an empty list"):
        process_field_encryption(field_name, values, cipher, config, mock_logger)


def test_process_field_encryption_single_element_list(mock_logger, config, cipher):
    field_name = 'test_field'
    values = ['single_value']

    result = process_field_encryption(field_name, values, cipher, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] != values[0]


# ============================================================================
# process_field_decryption TESTS
# ============================================================================

def test_process_field_decryption_single_value(mock_logger, config, cipher):
    field_name = 'nhs_number'
    plaintext = 'NHS123456789'
    encrypted = encrypt_value(plaintext, field_name, cipher, config, mock_logger)

    result = process_field_decryption(field_name, encrypted, cipher, config, mock_logger)

    assert isinstance(result, str)
    assert result == plaintext


def test_process_field_decryption_list_of_values(mock_logger, config, cipher):
    field_name = 'emails'
    plaintexts = ['test1@example.com', 'test2@example.com', 'test3@example.com']
    encrypted_list = [encrypt_value(p, field_name, cipher, config, mock_logger) for p in plaintexts]

    result = process_field_decryption(field_name, encrypted_list, cipher, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == 3
    assert result == plaintexts


def test_process_field_decryption_single_element_list(mock_logger, config, cipher):
    field_name = 'test_field'
    plaintext = 'single_value'
    encrypted = encrypt_value(plaintext, field_name, cipher, config, mock_logger)
    encrypted_list = [encrypted]

    result = process_field_decryption(field_name, encrypted_list, cipher, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0] == plaintext


@pytest.mark.parametrize('values_count', [1, 5, 10])
def test_process_field_decryption_list_various_sizes(mock_logger, config, cipher, values_count):
    field_name = 'test_field'
    plaintexts = [f'value_{i}' for i in range(values_count)]
    encrypted_list = [encrypt_value(p, field_name, cipher, config, mock_logger) for p in plaintexts]

    result = process_field_decryption(field_name, encrypted_list, cipher, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == values_count
    assert result == plaintexts


# ============================================================================
# validate_env_vars TESTS
# ============================================================================

def test_validate_env_vars_success(mock_logger, env_vars_set):
    validate_env_vars(mock_logger)


def test_validate_env_vars_missing_kms_key(mock_logger):
    with patch.dict(os.environ, {
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


def test_validate_env_vars_missing_key_versions(mock_logger):
    with patch.dict(os.environ, {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


def test_validate_env_vars_missing_algorithm(mock_logger):
    with patch.dict(os.environ, {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret'
    }, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


def test_validate_env_vars_missing_multiple(mock_logger):
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


# ============================================================================
# validate_event TESTS
# ============================================================================

@pytest.mark.parametrize('action', ['encrypt', 'reidentify'])
def test_validate_event_success(mock_logger, action):
    event = {
        'action': action,
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }
    validate_event(event, mock_logger)


@pytest.mark.parametrize('missing_field', ['action', 'field_name', 'field_value'])
def test_validate_event_missing_fields(mock_logger, missing_field):
    event = {
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }
    del event[missing_field]

    with pytest.raises(ValueError, match="Missing required event fields"):
        validate_event(event, mock_logger)


@pytest.mark.parametrize('invalid_action', ['invalid_action', 'decrypt', 'process', ''])
def test_validate_event_invalid_action(mock_logger, invalid_action):
    event = {
        'action': invalid_action,
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }
    with pytest.raises(ValueError, match="Invalid action"):
        validate_event(event, mock_logger)


def test_validate_event_none_field_value(mock_logger):
    event = {
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': None
    }
    with pytest.raises(ValueError, match="Missing required event fields"):
        validate_event(event, mock_logger)
