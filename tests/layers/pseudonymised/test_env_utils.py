import os
from unittest.mock import patch

import pytest

from env_utils import get_env_variables


def test_get_env_variables_returns_all_required_variables():
    env_vars = {
        'INPUT_S3_BUCKET': 'test-input-bucket',
        'INPUT_PREFIX': 'test-prefix',
        'OUTPUT_S3_BUCKET': 'test-output-bucket',
        'PSEUDONYMISATION_LAMBDA_FUNCTION_NAME': 'test-lambda',
        'KMS_KEY_ID': 'test-kms-key-id'
    }

    with patch.dict(os.environ, env_vars, clear=True):
        result = get_env_variables()

        assert result == env_vars


def test_get_env_variables_raises_value_error_when_variables_missing():
    env_vars = {
        'INPUT_S3_BUCKET': 'test-input-bucket',
        'INPUT_PREFIX': '',
        'OUTPUT_S3_BUCKET': 'test-output-bucket'
    }

    with patch.dict(os.environ, env_vars, clear=True):
        with pytest.raises(ValueError) as exc_info:
            get_env_variables()

        assert "Missing required environment variables" in str(exc_info.value)
        assert "INPUT_PREFIX" in str(exc_info.value)
