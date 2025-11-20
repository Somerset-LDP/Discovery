import os
from unittest.mock import patch

import pytest

from env_utils import get_env_variables


def test_get_env_variables_returns_all_required_variables():
    env_vars = {
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda',
        'KMS_KEY_ID': 'test-kms-key-id'
    }

    with patch.dict(os.environ, env_vars, clear=True):
        result = get_env_variables()

        assert result == env_vars
        assert result['PSEUDONYMISATION_LAMBDA_FUNCTION_NAME'] == 'test-lambda'
        assert result['KMS_KEY_ID'] == 'test-kms-key-id'


def test_get_env_variables_raises_value_error_when_variables_missing():
    env_vars = {
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda',
        'KMS_KEY_ID': ''
    }

    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(ValueError) as exc_info:
            get_env_variables()

        assert "Missing required environment variables" in str(exc_info.value)
        assert "KMS_KEY_ID" in str(exc_info.value)


def test_get_env_variables_raises_value_error_when_all_variables_missing():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError) as exc_info:
            get_env_variables()

        assert "Missing required environment variables" in str(exc_info.value)
        assert "PSEUDONYMISATION_LAMBDA_FUNCTION_NAME" in str(exc_info.value)
        assert "KMS_KEY_ID" in str(exc_info.value)

