import os
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from lambda_functions.pseudonymisation.pseudonymisation import (
    lambda_handler,
    key_cache,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(autouse=True)
def reset_cache():
    key_cache.clear()
    yield
    key_cache.clear()


def mock_get_secret(SecretId):
    if 'kms' in SecretId:
        return {'SecretString': 'arn:aws:kms:region:account:key/id'}
    return {'SecretString': '{"key_versions": "{\\"current\\": \\"v1\\"}"}'}


@pytest.fixture
def mock_everything():
    with patch.dict(os.environ, {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }):
        with patch('lambda_functions.pseudonymisation.pseudonymisation.secrets_client') as mock_secrets, \
             patch('lambda_functions.pseudonymisation.pseudonymisation.kms_client') as mock_kms:

            mock_secrets.get_secret_value.side_effect = mock_get_secret
            mock_kms.generate_data_key.return_value = {'Plaintext': b'0' * 32}

            yield mock_secrets, mock_kms


@pytest.fixture
def context():
    return MagicMock()


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

def test_integration_encrypt_single_value(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }, context)

    assert 'error' not in response, f"Got error: {response.get('error')}"
    assert response['field_name'] == 'nhs_number'
    assert isinstance(response['field_value'], str)


def test_integration_encrypt_list_values(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'emails',
        'field_value': ['test1@example.com', 'test2@example.com']
    }, context)

    assert 'error' not in response
    assert response['field_name'] == 'emails'
    assert isinstance(response['field_value'], list)


def test_integration_missing_action(mock_everything, context):
    response = lambda_handler({
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }, context)

    assert 'error' in response


def test_integration_missing_field_name(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_value': 'NHS123456789'
    }, context)

    assert 'error' in response


def test_integration_missing_field_value(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'nhs_number'
    }, context)

    assert 'error' in response


def test_integration_invalid_action(mock_everything, context):
    response = lambda_handler({
        'action': 'invalid',
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }, context)

    assert 'error' in response
    assert 'Invalid action' in response['error']


def test_integration_empty_value(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': ''
    }, context)

    assert 'error' in response


def test_integration_empty_list(mock_everything, context):
    response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'emails',
        'field_value': []
    }, context)

    assert 'error' in response


def test_integration_missing_env_vars(context):
    with patch.dict(os.environ, {}, clear=True):
        response = lambda_handler({
            'action': 'encrypt',
            'field_name': 'nhs_number',
            'field_value': 'NHS123456789'
        }, context)

        assert 'error' in response


def test_integration_kms_error(context):
    with patch.dict(os.environ, {
        'SECRET_NAME_KMS_KEY': 'kms-key-secret',
        'SECRET_NAME_KEY_VERSIONS': 'key-versions-secret',
        'ALGORITHM_ID': 'AES-SIV'
    }):
        with patch('lambda_functions.pseudonymisation.pseudonymisation.secrets_client') as mock_secrets, \
             patch('lambda_functions.pseudonymisation.pseudonymisation.kms_client') as mock_kms:

            mock_secrets.get_secret_value.side_effect = mock_get_secret
            mock_kms.generate_data_key.side_effect = ClientError(
                {'Error': {'Code': 'InvalidKeyId.NotFound'}},
                'GenerateDataKey'
            )

            response = lambda_handler({
                'action': 'encrypt',
                'field_name': 'nhs_number',
                'field_value': 'NHS123456789'
            }, context)

            assert 'error' in response


def test_integration_roundtrip_single(mock_everything, context):
    encrypt_response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': 'NHS123456789'
    }, context)

    assert 'error' not in encrypt_response, f"Encrypt failed: {encrypt_response.get('error')}"
    encrypted = encrypt_response['field_value']

    decrypt_response = lambda_handler({
        'action': 'reidentify',
        'field_name': 'nhs_number',
        'field_value': encrypted
    }, context)

    assert 'error' not in decrypt_response, f"Decrypt failed: {decrypt_response.get('error')}"
    assert decrypt_response['field_value'] == 'NHS123456789'


def test_integration_roundtrip_list(mock_everything, context):
    plaintexts = ['email1@test.com', 'email2@test.com']

    encrypt_response = lambda_handler({
        'action': 'encrypt',
        'field_name': 'emails',
        'field_value': plaintexts
    }, context)

    assert 'error' not in encrypt_response, f"Encrypt failed: {encrypt_response.get('error')}"
    encrypted_list = encrypt_response['field_value']

    decrypt_response = lambda_handler({
        'action': 'reidentify',
        'field_name': 'emails',
        'field_value': encrypted_list
    }, context)

    assert 'error' not in decrypt_response, f"Decrypt failed: {decrypt_response.get('error')}"
    assert decrypt_response['field_value'] == plaintexts
