import pytest

from location.aws_lambda.functions.checksum.path_utils import parse_landing_path


@pytest.mark.parametrize("object_key,expected_dataset_key,expected_file_name,expected_bronze_key", [
    (
            "landing/reference/onspd/202402/ONSPD_FEB_2024_UK.csv",
            "reference/onspd",
            "ONSPD_FEB_2024_UK.csv",
            "bronze/reference/onspd/202402/ONSPD_FEB_2024_UK.csv"
    ),
    (
            "landing/reference/imd2019/2019/IMD2019_English_LSOA.csv",
            "reference/imd2019",
            "IMD2019_English_LSOA.csv",
            "bronze/reference/imd2019/2019/IMD2019_English_LSOA.csv"
    ),
    (
            "landing/reference/dataset/2025/01/15/file.parquet",
            "reference/dataset",
            "file.parquet",
            "bronze/reference/dataset/2025/01/15/file.parquet"
    ),
])
def test_parse_landing_path_extracts_correct_components(object_key, expected_dataset_key, expected_file_name,
                                                        expected_bronze_key):
    result = parse_landing_path(object_key)

    assert result is not None
    assert result.dataset_key == expected_dataset_key
    assert result.file_name == expected_file_name
    assert result.full_key == object_key
    assert result.bronze_key == expected_bronze_key


@pytest.mark.parametrize("invalid_path", [
    "",
    None,
    "landing/reference/dataset/",
    "bronze/reference/onspd/file.csv",
    "other/reference/onspd/file.csv",
    "landing/other/onspd/file.csv",
    "landing/reference",
    "landing/reference/dataset",
])
def test_parse_landing_path_returns_none_for_invalid_paths(invalid_path):
    result = parse_landing_path(invalid_path)

    assert result is None
