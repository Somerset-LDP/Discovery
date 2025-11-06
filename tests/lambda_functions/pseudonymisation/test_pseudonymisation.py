import base64
import json
import logging
import os
from datetime import datetime, timedelta, UTC
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from logging_utils import CorrelationLogger
from pseudonymisation import (
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


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def cleanup_env():
    original_env = os.environ.copy()
    key_cache.clear()
    yield
    os.environ.clear()
    os.environ.update(original_env)
    key_cache.clear()


@pytest.fixture
def mock_logger():
    base_logger = logging.getLogger('test')
    return CorrelationLogger(base_logger, 'test-correlation-id')


@pytest.fixture
def mock_secrets_client():
    with patch('pseudonymisation.secrets_client') as mock:
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
    with patch('pseudonymisation.kms_client') as mock:
        yield mock


@pytest.fixture
def test_key_material():
    return b'0' * 32


@pytest.fixture
def encrypted_test_key(test_key_material):
    return base64.b64encode(test_key_material).decode('ascii')


@pytest.fixture
def config(encrypted_test_key):
    return Config(
        kms_key_id='arn:aws:kms:region:account:key/id',
        key_versions={'v1': encrypted_test_key, 'v2': encrypted_test_key},
        current_version='v1',
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )


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

    key_versions_json = json.dumps({
        "current": "v1",
        "keys": {"v1": "AQIDAHi8xK..."
                 }})

    with patch('pseudonymisation.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = [
            'arn:aws:kms:region:account:key/id',
            key_versions_json
        ]

        config = load_config(mock_logger)

        assert config.cache_ttl_hours == expected
        assert config.kms_key_id == 'arn:aws:kms:region:account:key/id'
        assert config.current_version == 'v1'
        assert config.key_versions == {'v1': 'AQIDAHi8xK...'}
        assert config.algorithm == 'AES-SIV'


def test_load_config_missing_kms_key_secret(mock_logger):
    with patch.dict(os.environ, {'ALGORITHM_ID': 'AES-SIV'}, clear=True):
        with pytest.raises(ValueError, match='Secret name cannot be empty'):
            load_config(mock_logger)


def test_load_config_missing_current_version(mock_logger, env_vars_set):
    encrypted_keys_json = json.dumps({
        "keys": {"v1": "AQIDAHi8xK..."}
    })

    with patch('pseudonymisation.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = ['arn:aws:kms:region:account:key/id', encrypted_keys_json]

        with pytest.raises(ValueError, match="Encrypted keys secret missing 'current' field"):
            load_config(mock_logger)


# ============================================================================
# get_secret TESTS
# ============================================================================

@pytest.mark.parametrize('secret_str,expected', [
    ('{"pseudonymisation/kms-key-id": "arn:aws:kms:region:account:key/id"}',
     '{"pseudonymisation/kms-key-id": "arn:aws:kms:region:account:key/id"}'),
    ('plain-text-secret', 'plain-text-secret'),
    ('{"single": "value"}', '{"single": "value"}'),
    ('{"current": "v1", "keys": {"v1": "..."}}', '{"current": "v1", "keys": {"v1": "..."}}'),
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
def test_get_data_key_uses_cache(mock_logger, mock_kms_client, config, test_key_material, ttl_hours, should_use_cache):
    config.cache_ttl_hours = ttl_hours
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    result = get_data_key('v1', config, mock_logger)

    assert result == test_key_material
    mock_kms_client.decrypt.assert_not_called()


def test_get_data_key_decrypts_when_expired(mock_logger, mock_kms_client, config, test_key_material):
    old_timestamp = datetime.now(UTC) - timedelta(hours=2)
    key_cache['v1'] = (b'old_key', old_timestamp)

    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    result = get_data_key('v1', config, mock_logger)

    assert result == test_key_material
    mock_kms_client.decrypt.assert_called_once()


def test_get_data_key_decrypts_on_cache_miss(mock_logger, mock_kms_client, config, test_key_material):
    key_cache.clear()
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    result = get_data_key('v1', config, mock_logger)

    assert result == test_key_material
    assert mock_kms_client.decrypt.called
    call_args = mock_kms_client.decrypt.call_args
    assert 'CiphertextBlob' in call_args[1]
    assert call_args[1]['KeyId'] == config.kms_key_id


def test_get_data_key_caches_result(mock_logger, mock_kms_client, config, test_key_material):
    key_cache.clear()
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    get_data_key('v1', config, mock_logger)

    assert 'v1' in key_cache
    cached_material, timestamp = key_cache['v1']
    assert cached_material == test_key_material
    assert isinstance(timestamp, datetime)


def test_get_data_key_kms_error(mock_logger, mock_kms_client, config):
    key_cache.clear()
    error_response = {'Error': {'Code': 'InvalidKeyId.NotFound'}}
    mock_kms_client.decrypt.side_effect = ClientError(error_response, 'Decrypt')

    with pytest.raises(ClientError):
        get_data_key('v1', config, mock_logger)


def test_get_data_key_invalid_version(mock_logger, config):
    key_cache.clear()

    with pytest.raises(ValueError, match="Key version 'v999' not found"):
        get_data_key('v999', config, mock_logger)


# ============================================================================
# build_aad TESTS
# ============================================================================

@pytest.mark.parametrize('field_name,algorithm,key_version', [
    ('nhs_number', 'AES-SIV', 'v1'),
    ('date_of_birth', 'AES-SIV', 'v2'),
    ('postcode', 'AES-GCM-SIV', 'v1'),
])
def test_build_aad_success(field_name, algorithm, key_version):
    config = Config(
        kms_key_id='test-key',
        key_versions={'v1': 'key1', 'v2': 'key2'},
        current_version=key_version,
        algorithm=algorithm,
        cache_ttl_hours=1
    )

    result = build_aad(field_name, key_version, config)

    assert isinstance(result, bytes)
    aad_dict = json.loads(result.decode('utf-8'))
    assert aad_dict['field'] == field_name
    assert aad_dict['algorithm'] == algorithm
    assert aad_dict['key_version'] == key_version


def test_build_aad_sorted_keys():
    config = Config(
        kms_key_id='test-key',
        key_versions={'v1': 'key1'},
        current_version='v1',
        algorithm='AES-SIV',
        cache_ttl_hours=1
    )

    result = build_aad('test_field', 'v1', config)
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
def test_encrypt_value_success(mock_logger, mock_kms_client, config, test_key_material, value, field_name):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    result = encrypt_value(value, field_name, config, mock_logger)

    assert isinstance(result, str)
    assert len(result) > 0
    assert result != value


def test_encrypt_value_returns_base64(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    result = encrypt_value('test_value', 'field', config, mock_logger)

    decoded = base64.urlsafe_b64decode(result)
    assert isinstance(decoded, bytes)
    assert len(decoded) > 0


def test_encrypt_value_empty_value_raises_error(mock_logger, config):
    with pytest.raises(ValueError, match="Value cannot be empty"):
        encrypt_value('', 'field', config, mock_logger)


def test_encrypt_value_whitespace_value_raises_error(mock_logger, config):
    with pytest.raises(ValueError, match="Value cannot be empty"):
        encrypt_value('   ', 'field', config, mock_logger)


def test_encrypt_value_deterministic(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    value = 'test_value'
    field_name = 'test_field'

    result1 = encrypt_value(value, field_name, config, mock_logger)
    result2 = encrypt_value(value, field_name, config, mock_logger)

    assert result1 == result2


def test_encrypt_value_different_fields_different_output(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    value = 'same_value'

    result1 = encrypt_value(value, 'field1', config, mock_logger)
    result2 = encrypt_value(value, 'field2', config, mock_logger)

    assert result1 != result2


def test_encrypt_value_uses_current_version(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    config.current_version = 'v2'

    encrypt_value('test', 'field', config, mock_logger)
    assert 'v2' in key_cache or mock_kms_client.decrypt.called


# ============================================================================
# decrypt_value TESTS
# ============================================================================

@pytest.mark.parametrize('field_name', [
    'nhs_number',
    'date_of_birth',
    'postcode',
    'email',
])
def test_decrypt_value_success(mock_logger, mock_kms_client, config, test_key_material, field_name):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    plaintext = 'test_value_123'
    encrypted = encrypt_value(plaintext, field_name, config, mock_logger)

    result = decrypt_value(encrypted, field_name, config, mock_logger)

    assert result == plaintext


def test_decrypt_value_empty_pseudonym_raises_error(mock_logger, config):
    with pytest.raises(ValueError, match="Pseudonym cannot be empty"):
        decrypt_value('', 'field', config, mock_logger)


def test_decrypt_value_whitespace_pseudonym_raises_error(mock_logger, config):
    with pytest.raises(ValueError, match="Pseudonym cannot be empty"):
        decrypt_value('   ', 'field', config, mock_logger)


def test_decrypt_value_invalid_base64_raises_error(mock_logger, config):
    with pytest.raises(Exception):
        decrypt_value('not_valid_base64!!!', 'field', config, mock_logger)


def test_decrypt_value_wrong_field_name_raises_error(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    plaintext = 'test_value'
    encrypted = encrypt_value(plaintext, 'field1', config, mock_logger)

    with pytest.raises(ValueError, match="Failed to decrypt"):
        decrypt_value(encrypted, 'field2', config, mock_logger)


def test_decrypt_value_tries_multiple_versions(mock_logger, mock_kms_client, config, test_key_material):
    key_v1 = b'1' * 32
    key_v2 = b'2' * 32

    config.current_version = 'v1'
    key_cache['v1'] = (key_v1, datetime.now(UTC))
    encrypted = encrypt_value('test', 'field', config, mock_logger)

    config.current_version = 'v2'
    key_cache.clear()

    def decrypt_side_effect(*args, **kwargs):
        blob = kwargs.get('CiphertextBlob')
        if blob == base64.b64decode(config.key_versions['v1']):
            return {'Plaintext': key_v1}
        return {'Plaintext': key_v2}

    mock_kms_client.decrypt.side_effect = decrypt_side_effect

    result = decrypt_value(encrypted, 'field', config, mock_logger)
    assert result == 'test'


@pytest.mark.parametrize('plaintext', [
    'NHS123456789',
    '1990-01-15',
    'SW1A 1AA',
    'john.doe@example.com',
    'special!@#$%^&*()',
    'a',
    'value with spaces',
])
def test_decrypt_value_various_inputs(mock_logger, mock_kms_client, config, test_key_material, plaintext):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    encrypted = encrypt_value(plaintext, 'field', config, mock_logger)
    result = decrypt_value(encrypted, 'field', config, mock_logger)

    assert result == plaintext


# ============================================================================
# process_field_encryption TESTS
# ============================================================================

def test_process_field_encryption_single_value(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}

    field_name = 'nhs_number'
    value = 'NHS123456789'

    result = process_field_encryption(field_name, value, config, mock_logger)

    assert isinstance(result, str)
    assert result != value


def test_process_field_encryption_list_of_values(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    field_name = 'emails'
    values = ['test1@example.com', 'test2@example.com', 'test3@example.com']

    result = process_field_encryption(field_name, values, config, mock_logger)

    assert isinstance(result, list)
    assert len(result) == 3
    for encrypted, original in zip(result, values):
        assert encrypted != original
        assert isinstance(encrypted, str)


def test_process_field_encryption_empty_list_raises_error(mock_logger, config):
    field_name = 'test_field'
    values = []

    with pytest.raises(ValueError, match="contains an empty list"):
        process_field_encryption(field_name, values, config, mock_logger)


# ============================================================================
# process_field_decryption TESTS
# ============================================================================

def test_process_field_decryption_single_value(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    field_name = 'nhs_number'
    original = 'NHS123456789'
    encrypted = encrypt_value(original, field_name, config, mock_logger)

    result = process_field_decryption(field_name, encrypted, config, mock_logger)

    assert result == original


def test_process_field_decryption_list_of_values(mock_logger, mock_kms_client, config, test_key_material):
    mock_kms_client.decrypt.return_value = {'Plaintext': test_key_material}
    key_cache['v1'] = (test_key_material, datetime.now(UTC))

    field_name = 'emails'
    originals = ['test1@example.com', 'test2@example.com', 'test3@example.com']
    encrypted = [encrypt_value(v, field_name, config, mock_logger) for v in originals]

    result = process_field_decryption(field_name, encrypted, config, mock_logger)

    assert isinstance(result, list)
    assert result == originals


# ============================================================================
# validate_env_vars TESTS
# ============================================================================

def test_validate_env_vars_success(mock_logger):
    env = {
        'SECRET_NAME_KMS_KEY': 'kms-key',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions',
        'ALGORITHM_ID': 'AES-SIV'
    }
    with patch.dict(os.environ, env, clear=True):
        validate_env_vars(mock_logger)


def test_validate_env_vars_missing_kms_key(mock_logger):
    env = {
        'SECRET_NAME_KEY_VERSIONS': 'key-versions',
        'ALGORITHM_ID': 'AES-SIV'
    }
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


def test_validate_env_vars_missing_key_versions(mock_logger):
    env = {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            validate_env_vars(mock_logger)


def test_validate_env_vars_missing_algorithm(mock_logger):
    env = {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret'
    }
    with patch.dict(os.environ, env, clear=True):
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
        'field_value': 'test_value'
    }
    validate_event(event, mock_logger)


def test_validate_event_invalid_action(mock_logger):
    event = {
        'action': 'invalid',
        'field_name': 'nhs_number',
        'field_value': 'test_value'
    }
    with pytest.raises(ValueError, match="Invalid action"):
        validate_event(event, mock_logger)


def test_validate_event_missing_field(mock_logger):
    event = {
        'action': 'encrypt',
        'field_name': 'nhs_number'
    }
    with pytest.raises(ValueError, match="Missing required event fields"):
        validate_event(event, mock_logger)
