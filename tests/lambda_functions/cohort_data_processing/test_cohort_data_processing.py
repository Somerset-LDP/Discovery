import hashlib
import pandas as pd
import pytest
import os
from unittest.mock import patch
from lambda_functions.cohort_data_processing.cohort_data_processing import (
    validate_checksum,
    delete_and_log_remaining,
    is_valid_nhs_number,
    clean_and_validate_nhs_df,
    load_and_clean_nhs_csv,
    get_env_variables,
    pseudonymise_nhs_numbers,
    REQUIRED_ENV_VARS, lambda_handler
)


@pytest.fixture(autouse=True)
def cleanup_env():
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


def test_validate_checksum():
    content = b"abc123"
    actual_checksum = hashlib.sha256(content).hexdigest()
    checksum_content = actual_checksum.encode("utf-8")
    try:
        validate_checksum(content, checksum_content, "test-key")
    except Exception as e:
        pytest.fail(f"validate_checksum raised an exception unexpectedly: {e}")


def test_validate_checksum_mismatch():
    content = b"abc123"
    wrong_checksum = "deadbeef" * 8
    checksum_content = wrong_checksum.encode("utf-8")
    with pytest.raises(ValueError) as excinfo:
        validate_checksum(content, checksum_content, "test-key")
    assert "Checksum mismatch" in str(excinfo.value)


def test_validate_checksum_decode_error():
    content = b"abc123"
    checksum_content = b"\xff\xfe\xfd\xfc"
    with pytest.raises(Exception) as excinfo:
        validate_checksum(content, checksum_content, "test-key")
    assert (
        "Error in checksum validation" in str(excinfo.value)
        or excinfo.type is UnicodeDecodeError
    )


def test_delete_and_log_remaining(caplog):
    with patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.delete_s3_objects"
    ) as mock_delete, patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.list_s3_files"
    ) as mock_list:
        mock_list.return_value = []
        with caplog.at_level("INFO"):
            delete_and_log_remaining("bucket", ["file1.csv", "file2.csv"], "prefix")
        mock_delete.assert_called_once_with("bucket", ["file1.csv", "file2.csv"])
        mock_list.assert_called_once_with("bucket", "prefix")
        assert any(
            "All source files deleted" in m for m in caplog.messages
        )


def test_delete_and_log_remaining_with_remaining_files(caplog):
    with patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.delete_s3_objects"
    ) as mock_delete, patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.list_s3_files"
    ) as mock_list:
        mock_list.return_value = ["prefix/file3.csv"]
        with caplog.at_level("WARNING"):
            delete_and_log_remaining("bucket", ["file1.csv"], "prefix")
        assert any(
            "Not all files deleted" in m for m in caplog.messages
        )


@pytest.mark.parametrize(
    "nhs_number,expected",
    [
        ("943 476 5919", True),  # valid with spaces
        (9434765919, True),      # valid as int
        ("9434765919", True),    # valid as string
        (" 9434765919 ", True),  # valid with leading/trailing spaces
        ("9434765910", False),   # invalid check digit
        ("943476591", False),    # too short
        ("94347659199", False),  # too long
        ("abcdefghij", False),   # non-numeric
        ("94347 6591a", False),  # contains letter
        ("", False),             # empty string
        (None, False),           # None input
        ("  ", False),           # only spaces
        ("1234567890", False),   # valid length, invalid check digit
        ("9434765918", False),   # valid length, invalid check digit
    ],
)
def test_is_valid_nhs_number(nhs_number, expected):
    assert is_valid_nhs_number(nhs_number) == expected


@pytest.mark.parametrize(
    "input_data,expected_nhs",
    [
        (
            ["943 476 5919", 9434765919, "9434765919", " 9434765919 "],
            ["9434765919", "9434765919", "9434765919", "9434765919"],
        ),
        (["9434765910", "943476591", "94347659199", "abcdefghij", "", "  "], []),
        (
            ["9434765919", "9434765910", "abcdefghij", "943 476 5919"],
            ["9434765919", "9434765919"],
        ),
        (
            [" 9434765919 ", "943 476 5919"],
            ["9434765919", "9434765919"],
        ),
        ([None, "", "  ", "abcdefghij", "1234567890"], []),
    ],
)
def test_clean_and_validate_nhs_df(input_data, expected_nhs):
    df = pd.DataFrame({"nhs": input_data})
    cleaned = clean_and_validate_nhs_df(df.copy(), "nhs")
    assert sorted(list(cleaned["nhs"])) == sorted(expected_nhs)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv(mock_validate_checksum, mock_get_s3_object_content):
    sft_csv = "943 476 5919\n9434765910\n 8314495581 \nabcdefghij\n943 476 5919\n"
    sft_bytes = sft_csv.encode("utf-8")
    checksum_bytes = b"fakechecksum sft.csv"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    df = load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')
    assert sorted(list(df["nhs"])) == ["8314495581", "9434765919"]
    mock_validate_checksum.assert_called_once_with(sft_bytes, checksum_bytes, "key")


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv_extra_columns(mock_validate_checksum, mock_get_s3_object_content):
    sft_csv = "9434765919,extra\n8132262247,column\n"
    sft_bytes = sft_csv.encode("utf-8")
    checksum_bytes = b"fakechecksum"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    df = load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')
    assert sorted(list(df["nhs"])) == ["8132262247", "9434765919"]


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv_invalid_utf8(mock_validate_checksum, mock_get_s3_object_content):
    sft_bytes = b"\xff\xfe\xfd\xfc"
    checksum_bytes = b"fakechecksum"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    with pytest.raises(UnicodeDecodeError):
        load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv_checksum_fail(mock_validate_checksum, mock_get_s3_object_content):
    sft_bytes = b"9434765919\n"
    checksum_bytes = b"badchecksum"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    mock_validate_checksum.side_effect = ValueError("Checksum mismatch")
    with pytest.raises(ValueError):
        load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv_empty_file(mock_validate_checksum, mock_get_s3_object_content):
    sft_bytes = b""
    checksum_bytes = b"fakechecksum"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    with pytest.raises(ValueError) as excinfo:
        load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')
    assert "is empty" in str(excinfo.value)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_s3_object_content")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.validate_checksum")
def test_load_and_clean_nhs_csv_removes_duplicates(mock_validate_checksum, mock_get_s3_object_content):
    sft_csv = "9434765919\n9434765919\n9434765919\n"
    sft_bytes = sft_csv.encode("utf-8")
    checksum_bytes = b"fakechecksum"
    mock_get_s3_object_content.side_effect = [sft_bytes, checksum_bytes]
    df = load_and_clean_nhs_csv("bucket", "key", "cbucket", "ckey", filetype='SFT')
    assert list(df["nhs"]) == ["9434765919"]
    assert len(df) == 1


def test_get_env_variables_all_present():
    for var in REQUIRED_ENV_VARS:
        os.environ[var] = f"value_for_{var}"
    result = get_env_variables()
    for var in REQUIRED_ENV_VARS:
        assert result[var] == f"value_for_{var}"


def test_get_env_variables_missing():
    for var in REQUIRED_ENV_VARS:
        os.environ.pop(var, None)
    if REQUIRED_ENV_VARS:
        os.environ[REQUIRED_ENV_VARS[0]] = "some_value"
    with pytest.raises(KeyError) as excinfo:
        get_env_variables()
    missing = [v for v in REQUIRED_ENV_VARS if v not in os.environ]
    for mv in missing:
        assert mv in str(excinfo.value)


def test_get_env_variables_strips_whitespace():
    for var in REQUIRED_ENV_VARS:
        os.environ[var] = f"  value_for_{var}  "
    result = get_env_variables()
    for var in REQUIRED_ENV_VARS:
        assert result[var] == f"value_for_{var}"
        assert not result[var].startswith(" ")
        assert not result[var].endswith(" ")


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_success(mock_invoke_lambda):
    nhs_numbers = {"9434765919", "8314495581", "8132262247"}
    lambda_function = "test-pseudo-lambda"
    mock_invoke_lambda.return_value = {
        "field_name": "nhs_number",
        "field_value": ["pseudo_1", "pseudo_2", "pseudo_3"]
    }

    result = pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    mock_invoke_lambda.assert_called_once()
    call_args = mock_invoke_lambda.call_args[0]
    assert call_args[0] == lambda_function

    payload = call_args[1]
    assert payload["action"] == "encrypt"
    assert payload["field_name"] == "nhs_number"
    assert len(payload["field_value"]) == 3
    assert all(nhs in payload["field_value"] for nhs in nhs_numbers)

    assert isinstance(result, set)
    assert len(result) == 3
    assert result == {"pseudo_1", "pseudo_2", "pseudo_3"}


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_empty_set(mock_invoke_lambda, caplog):
    nhs_numbers = set()
    lambda_function = "test-pseudo-lambda"

    result = pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    mock_invoke_lambda.assert_not_called()
    assert result == set()
    assert any("Empty set provided" in m for m in caplog.messages)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_lambda_error(mock_invoke_lambda):
    nhs_numbers = {"9434765919"}
    lambda_function = "test-pseudo-lambda"
    mock_invoke_lambda.return_value = {
        "error": "Encryption failed: invalid key"
    }

    with pytest.raises(ValueError) as excinfo:
        pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    assert "Pseudonymisation Lambda returned error" in str(excinfo.value)
    assert "invalid key" in str(excinfo.value)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_missing_field_value(mock_invoke_lambda):
    nhs_numbers = {"9434765919"}
    lambda_function = "test-pseudo-lambda"
    mock_invoke_lambda.return_value = {
        "field_name": "nhs_number"
    }

    with pytest.raises(ValueError) as excinfo:
        pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    assert "missing 'field_value'" in str(excinfo.value)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_count_mismatch(mock_invoke_lambda):
    nhs_numbers = {"9434765919", "8314495581", "8132262247"}
    lambda_function = "test-pseudo-lambda"
    mock_invoke_lambda.return_value = {
        "field_name": "nhs_number",
        "field_value": ["pseudo_1", "pseudo_2"]
    }

    with pytest.raises(ValueError) as excinfo:
        pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    assert "returned 2 values, expected 3" in str(excinfo.value)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.invoke_lambda")
def test_pseudonymise_nhs_numbers_preserves_order(mock_invoke_lambda):
    nhs_numbers = {"9434765919", "8132262247", "8314495581"}
    lambda_function = "test-pseudo-lambda"
    mock_invoke_lambda.return_value = {
        "field_name": "nhs_number",
        "field_value": ["pseudo_1", "pseudo_2", "pseudo_3"]
    }

    pseudonymise_nhs_numbers(nhs_numbers, lambda_function)

    payload = mock_invoke_lambda.call_args[0][1]
    nhs_list = payload["field_value"]
    assert nhs_list == sorted(nhs_list)


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.write_to_s3")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.delete_and_log_remaining")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.pseudonymise_nhs_numbers")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.load_and_clean_nhs_csv")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_files")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_env_variables")
def test_lambda_handler_integration(
    mock_get_env, mock_get_files, mock_load_csv,
    mock_pseudonymise, mock_delete, mock_write_s3
):
    mock_get_env.return_value = {
        "S3_SFT_FILE_PREFIX": "bucket/sft/",
        "S3_SFT_CHECKSUM_PREFIX": "bucket/sft-checksums/",
        "S3_GP_FILES_PREFIX": "bucket/gp/",
        "S3_GP_CHECKSUMS_PREFIX": "bucket/gp-checksums/",
        "S3_COHORT_KEY": "bucket/cohort/cohort.csv",
        "KMS_KEY_ID": "arn:aws:kms:eu-west-2:123456789012:key/test-key-id",
        "PSEUDONYMISATION_LAMBDA_FUNCTION_NAME": "pseudo-lambda"
    }

    mock_get_files.side_effect = [
        ("bucket", ["sft/file1.csv"]),
        ("bucket", ["sft-checksums/file1.sha256"]),
        ("bucket", ["gp/gp1.csv", "gp/gp2.csv"]),
        ("bucket", ["gp-checksums/gp1.sha256", "gp-checksums/gp2.sha256"])
    ]

    sft_df = pd.DataFrame({"nhs": ["9434765919", "8314495581", "8132262247", "9449304130", "9449304122"]})
    gp1_df = pd.DataFrame({"nhs": ["9434765919", "8314495581", "9999999999"]})
    gp2_df = pd.DataFrame({"nhs": ["8132262247", "9449304130", "8888888888"]})

    mock_load_csv.side_effect = [sft_df, gp1_df, gp2_df]
    mock_pseudonymise.return_value = {"pseudo_1", "pseudo_2", "pseudo_3", "pseudo_4"}

    result = lambda_handler({}, None)

    mock_pseudonymise.assert_called_once()
    pseudonymise_call_args = mock_pseudonymise.call_args[0]
    nhs_set = pseudonymise_call_args[0]
    lambda_name = pseudonymise_call_args[1]

    assert len(nhs_set) == 4
    assert lambda_name == "pseudo-lambda"

    mock_write_s3.assert_called_once()
    write_call_args = mock_write_s3.call_args[0]
    assert write_call_args[0] == "bucket"
    assert write_call_args[1] == "cohort/cohort.csv"
    assert write_call_args[2] == {"pseudo_1", "pseudo_2", "pseudo_3", "pseudo_4"}
    assert write_call_args[3] == "arn:aws:kms:eu-west-2:123456789012:key/test-key-id"

    assert mock_delete.call_count == 4

    assert result["final_count"] == 4
    assert result["pseudonymised_count"] == 4
    assert result["cohort_key"] == "cohort/cohort.csv"


@patch("lambda_functions.cohort_data_processing.cohort_data_processing.write_to_s3")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.delete_and_log_remaining")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.pseudonymise_nhs_numbers")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.load_and_clean_nhs_csv")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_files")
@patch("lambda_functions.cohort_data_processing.cohort_data_processing.get_env_variables")
def test_lambda_handler_no_intersections(
    mock_get_env, mock_get_files, mock_load_csv, mock_pseudonymise, mock_delete, mock_write_s3
):
    mock_get_env.return_value = {
        "S3_SFT_FILE_PREFIX": "bucket/sft/",
        "S3_SFT_CHECKSUM_PREFIX": "bucket/sft-checksums/",
        "S3_GP_FILES_PREFIX": "bucket/gp/",
        "S3_GP_CHECKSUMS_PREFIX": "bucket/gp-checksums/",
        "S3_COHORT_KEY": "bucket/cohort/cohort.csv",
        "KMS_KEY_ID": "arn:aws:kms:eu-west-2:123456789012:key/test-key-id",
        "PSEUDONYMISATION_LAMBDA_FUNCTION_NAME": "pseudo-lambda"
    }

    mock_get_files.side_effect = [
        ("bucket", ["sft/file1.csv"]),
        ("bucket", ["sft-checksums/file1.sha256"]),
        ("bucket", ["gp/gp1.csv"]),
        ("bucket", ["gp-checksums/gp1.sha256"])
    ]
    sft_df = pd.DataFrame({"nhs": ["9434765919", "8314495581"]})
    gp1_df = pd.DataFrame({"nhs": ["1111111111", "2222222222"]})

    mock_load_csv.side_effect = [sft_df, gp1_df]
    mock_pseudonymise.return_value = set()

    result = lambda_handler({}, None)

    mock_pseudonymise.assert_called_once()
    assert mock_pseudonymise.call_args[0][0] == set()
    assert mock_delete.call_count == 4
    assert result["final_count"] == 0
    assert result["pseudonymised_count"] == 0

