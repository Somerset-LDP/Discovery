""" One time script to generate presigned URLs for uploading cohort CSV files
    for a list of GP practices."""
import os
import sys
from typing import List

from dotenv import load_dotenv

from presigned_url_utils import generate_curl_to_upload

load_dotenv()


def generate_cohort_curls(
        gp_ods_codes: List[str],
        kms_key: str,
        file_path: str,
        expiration: int = 3600
) -> List[str]:
    curl_commands = []

    for gp_ods_code in gp_ods_codes:
        file_name = f"Over65_{gp_ods_code}.csv"
        print(f"Generating presigned URL for: {file_name}")

        try:
            curl_command = generate_curl_to_upload(
                kms_key=kms_key,
                file_path=file_path,
                file_name=file_name,
                expiration=expiration
            )
            curl_commands.append(curl_command)
        except Exception as e:
            print(f"Error generating URL for {file_name}: {e}")
            continue

    return curl_commands


def main():
    gp_ods_codes = [
        "L85055",
        "L85032",
        "L85016",
        "L85066",
        "L85007",
        "L85609",
        "L85004",
        "L85003",
        "L85008",
        "L85026",
        "L85010",
        "L85038",
        "L85056",
        "L85064",
        "L85061",
        "L85052",
        "L85027"
    ]

    kms_key = os.getenv("KMS_KEY")
    file_path = os.getenv("FILE_PATH")
    expiration = int(os.getenv("EXPIRATION", "3600"))

    if not kms_key:
        print("Error: KMS_KEY environment variable is required")
        sys.exit(1)

    if not file_path:
        print("Error: FILE_PATH environment variable is required")
        sys.exit(1)

    curl_commands = generate_cohort_curls(
        gp_ods_codes=gp_ods_codes,
        kms_key=kms_key,
        file_path=file_path,
        expiration=expiration
    )

    print(f"Successfully generated {len(curl_commands)} curl commands")
    for i, cmd in enumerate(curl_commands, 1):
        print(f"\n{i}. {cmd}")

    return curl_commands


if __name__ == "__main__":
    main()
