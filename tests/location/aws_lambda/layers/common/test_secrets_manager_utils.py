from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from location.aws_lambda.layers.common.common_utils import DataIngestionException
from location.aws_lambda.layers.common.secrets_manager_utils import get_secret_value


@patch('location.aws_lambda.layers.common.secrets_manager_utils.boto3.client')
def test_get_secret_value_returns_secret_string(mock_boto_client):
    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {'SecretString': 'my_secret_value'}
    mock_boto_client.return_value = mock_sm

    result = get_secret_value('my_secret_name')

    assert result == 'my_secret_value'
    mock_sm.get_secret_value.assert_called_once_with(SecretId='my_secret_name')


@patch('location.aws_lambda.layers.common.secrets_manager_utils.boto3.client')
def test_get_secret_value_raises_exception_on_client_error(mock_boto_client):
    mock_sm = MagicMock()
    mock_sm.get_secret_value.side_effect = ClientError(
        {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Secret not found'}},
        'GetSecretValue'
    )
    mock_boto_client.return_value = mock_sm

    with pytest.raises(DataIngestionException) as exc_info:
        get_secret_value('nonexistent_secret')

    assert "Failed to retrieve secret" in exc_info.value.message


@pytest.mark.parametrize("invalid_value", ["", None])
def test_get_secret_value_raises_exception_for_empty_secret_name(invalid_value):
    with pytest.raises(DataIngestionException) as exc_info:
        get_secret_value(invalid_value)

    assert "Secret name is required" in exc_info.value.message
