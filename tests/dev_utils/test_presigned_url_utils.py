import sys
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from dev_utils import presigned_url_utils


def test_generate_s3_presigned_url():
    with patch.object(presigned_url_utils, "s3_client") as mock_client:
        mock_client.generate_presigned_url.return_value = "https://example.com/presigned"

        url = presigned_url_utils.generate_s3_presigned_url(
            bucket="test-bucket",
            key="test-key",
            kms_key="kms-id",
            expiration=3600,
        )

        assert url == "https://example.com/presigned"
        mock_client.generate_presigned_url.assert_called_once_with(
            "put_object",
            Params={
                "Bucket": "test-bucket",
                "Key": "test-key",
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": "kms-id",
            },
            ExpiresIn=3600,
        )


def test_generate_s3_presigned_url_failure():
    with patch.object(presigned_url_utils, "s3_client") as mock_client:
        mock_client.generate_presigned_url.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Not allowed"}},
            "GeneratePresignedUrl"
        )

        with pytest.raises(ClientError):
            presigned_url_utils.generate_s3_presigned_url(
                bucket="test-bucket",
                key="test-key",
                kms_key="kms-id",
                expiration=3600,
            )


def test_generate_curl_to_upload():
    with patch.object(presigned_url_utils, "generate_s3_presigned_url",
                      return_value="https://example.com/presigned") as mock_presign:
        curl_command = presigned_url_utils.generate_curl_to_upload(
            kms_key="test-kms",
            file_path="my-bucket/prefix/",
            file_name="test.csv",
            expiration=3600
        )

        mock_presign.assert_called_once_with("my-bucket", "prefix/test.csv", "test-kms", 3600)

        assert "https://example.com/presigned" in curl_command
        assert "X-Amz-Server-Side-Encryption: aws:kms" in curl_command
        assert "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id: test-kms" in curl_command
        assert "--data-binary" in curl_command


def test_generate_curl_to_upload_invalid_path():
    with pytest.raises(ValueError) as excinfo:
        presigned_url_utils.generate_curl_to_upload(
            kms_key="test-kms",
            file_path="invalidpath",
            file_name="file.csv",
            expiration=3600
        )
    assert "Invalid file_path format" in str(excinfo.value)


def test_get_params_with_env_only():
    args = type("Args", (), {"kms_key": None, "file_path": None, "file_name": None, "expiration": None})()

    with patch.dict("os.environ", {
        "KMS_KEY": "env-kms",
        "FILE_PATH": "env-bucket/prefix",
        "FILE_NAME": "envfile.csv",
        "EXPIRATION": "7200"
    }):
        kms, path, name, exp = presigned_url_utils.get_params(args)
        assert kms == "env-kms"
        assert path == "env-bucket/prefix"
        assert name == "envfile.csv"
        assert exp == "7200"


def test_get_params_with_args_only():
    args = type("Args", (), {
        "kms_key": "arg-kms",
        "file_path": "arg-bucket/prefix",
        "file_name": "argfile.csv",
        "expiration": 3600
    })()

    kms, path, name, exp = presigned_url_utils.get_params(args)
    assert kms == "arg-kms"
    assert path == "arg-bucket/prefix"
    assert name == "argfile.csv"
    assert exp == 3600


def test_validate_params_all_present():
    try:
        presigned_url_utils.validate_params("kms-key", "bucket/prefix", "file.csv", 3600)
    except Exception as e:
        pytest.fail(f"validate_params raised an unexpected exception: {e}")


@pytest.mark.parametrize(
    "kms_key, file_path, file_name, expiration, missing_expected",
    [
        (None, "bucket/prefix", "file.csv", 3600, ["KMS_KEY / --kms-key"]),
        ("kms-key", None, "file.csv", 3600, ["FILE_PATH / --file-path"]),
        ("kms-key", "bucket/prefix", None, 3600, ["FILE_NAME / --file-name"]),
        ("kms-key", "bucket/prefix", "file.csv", None, ["EXPIRATION / --expiration"]),
        (None, None, None, None, [
            "KMS_KEY / --kms-key",
            "FILE_PATH / --file-path",
            "FILE_NAME / --file-name",
            "EXPIRATION / --expiration"
        ])
    ]
)
def test_validate_params_missing_params(kms_key, file_path, file_name, expiration, missing_expected):
    with patch.object(sys, "exit") as mock_exit, patch("builtins.print") as mock_print:
        presigned_url_utils.validate_params(kms_key, file_path, file_name, expiration)
        mock_exit.assert_called_once_with(1)
        printed_text = " ".join([args[0] for args, _ in mock_print.call_args_list])
        for missing in missing_expected:
            assert missing in printed_text


def test_parse_cli_args_all_args():
    test_argv = [
        "script_name.py",
        "--kms-key", "test-kms",
        "--file-path", "bucket/prefix",
        "--file-name", "file.csv",
        "--expiration", "3600"
    ]
    with patch.object(sys, "argv", test_argv):
        args = presigned_url_utils.parse_cli_args()
        assert args.kms_key == "test-kms"
        assert args.file_path == "bucket/prefix"
        assert args.file_name == "file.csv"
        assert args.expiration == 3600


def test_parse_cli_args_missing_args():
    test_argv = [
        "script_name.py",
        "--kms-key", "test-kms"
    ]
    with patch.object(sys, "argv", test_argv):
        args = presigned_url_utils.parse_cli_args()
        assert args.kms_key == "test-kms"
        assert args.file_path is None
        assert args.file_name is None
        assert args.expiration is None
