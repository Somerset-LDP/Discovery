import hashlib
import pandas as pd
import pytest
import os
from unittest.mock import patch
from lambda_functions.cohort_data_processing.cohort_data_processing import (
    validate_checksum,
    write_cohort,
    delete_and_log_remaining,
    is_valid_nhs_number,
    clean_and_validate_nhs_df,
    load_and_clean_nhs_csv,
    get_env_variables,
    REQUIRED_ENV_VARS
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


def test_write_cohort():
    with patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.write_to_s3"
    ) as mock_write:
        write_cohort("bucket", "key", {"1", "2"})
        mock_write.assert_called_once_with("bucket", "key", {"1", "2"})


def test_write_cohort_error():
    with patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.write_to_s3"
    ) as mock_write:
        mock_write.side_effect = Exception("fail")
        with pytest.raises(Exception) as excinfo:
            write_cohort("bucket", "key", {"1"})
        assert "fail" in str(excinfo.value)


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


def test_delete_and_log_remaining_error():
    with patch(
        "lambda_functions.cohort_data_processing.cohort_data_processing.delete_s3_objects"
    ) as mock_delete:
        mock_delete.side_effect = Exception("delete error")
        with pytest.raises(Exception) as excinfo:
            delete_and_log_remaining("bucket", ["file1.csv"], "prefix")
        assert "delete error" in str(excinfo.value)


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
