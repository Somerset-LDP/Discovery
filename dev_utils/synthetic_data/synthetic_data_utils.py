import csv
import hashlib
import os
import random
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import boto3
from dotenv import load_dotenv

from dev_utils.synthetic_data.data_generators import generate_valid_nhs_number, generate_invalid_nhs_number

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

s3_client = boto3.client('s3')


def generate_nhs_numbers(valid_count: int, invalid_count: int) -> List[str]:
    nhs_numbers = [generate_valid_nhs_number() for _ in range(valid_count)]
    nhs_numbers += [generate_invalid_nhs_number() for _ in range(invalid_count)]
    random.shuffle(nhs_numbers)
    return nhs_numbers


def create_csv_file(nhs_numbers: List[str], output_dir: str, prefix: str = "nhs") -> str:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    random_id = random.randint(1000, 9999)
    filename = f"{prefix}_{timestamp}_{random_id}.csv"
    file_path = os.path.join(output_dir, filename)
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for nhs in nhs_numbers:
            writer.writerow([nhs])
    return file_path


def generate_sha256_checksum(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    checksum_file = file_path.replace('.csv', ".sha256")
    with open(checksum_file, 'w') as f:
        f.write(f"{checksum}  {os.path.basename(file_path)}\n")
    return checksum_file


def upload_to_s3(file_path: str, bucket: str, key: str, kms_key_id: str) -> None:
    with open(file_path, 'rb') as f:
        s3_client.upload_fileobj(
            f,
            bucket,
            key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": kms_key_id
            }
        )
    print(f"Uploaded: s3://{bucket}/{key}")


def upload_file_and_checksum(
    file_path: str,
    bucket: str,
    file_prefix: str,
    checksum_prefix: str,
    kms_key_id: str
) -> None:
    filename = os.path.basename(file_path)
    checksum_filename = filename.replace('.csv', '.sha256')

    file_key = os.path.join(file_prefix, filename)
    upload_to_s3(file_path, bucket, file_key, kms_key_id)

    checksum_path = generate_sha256_checksum(file_path)
    checksum_key = os.path.join(checksum_prefix, checksum_filename)
    upload_to_s3(checksum_path, bucket, checksum_key, kms_key_id)

    os.remove(file_path)
    os.remove(checksum_path)


def generate_and_upload_sft_data(
    num_files: int = None,
    valid_per_file: int = None,
    invalid_per_file: int = None,
    bucket: str = None,
    file_prefix: str = None,
    checksum_prefix: str = None,
    kms_key_id: str = None,
    local_tmp_dir: str = None
) -> Tuple[List[str], int]:
    num_files = num_files or int(os.getenv("NUM_SFT_FILES", 1))
    valid_per_file = valid_per_file or int(os.getenv("VALID_NHS_PER_FILE", 2000))
    invalid_per_file = invalid_per_file or int(os.getenv("INVALID_NHS_PER_FILE", 300))
    bucket = bucket or os.getenv("S3_BUCKET")
    file_prefix = file_prefix or os.getenv("SFT_FILES_PREFIX")
    checksum_prefix = checksum_prefix or os.getenv("SFT_CHECKSUMS_PREFIX")
    kms_key_id = kms_key_id or os.getenv("KMS_KEY_ID")
    local_tmp_dir = local_tmp_dir or os.getenv("LOCAL_TMP_DIR", ".")

    print(f"Generating {num_files} SFT file(s)...")
    print(f"Valid NHS per file: {valid_per_file}")
    print(f"Invalid NHS per file: {invalid_per_file}")

    all_valid_numbers = []

    for i in range(num_files):
        nhs_numbers = generate_nhs_numbers(valid_per_file, invalid_per_file)
        all_valid_numbers.extend([n for n in nhs_numbers if len(n) == 10 and n.isdigit()])

        file_path = create_csv_file(nhs_numbers, local_tmp_dir, prefix="sft")
        upload_file_and_checksum(file_path, bucket, file_prefix, checksum_prefix, kms_key_id)

        print(f"   [{i+1}/{num_files}] Uploaded SFT file with {len(nhs_numbers)} NHS numbers")

    return all_valid_numbers, len(all_valid_numbers)


def generate_and_upload_gp_data(
    num_files: int = None,
    valid_per_file: int = None,
    invalid_per_file: int = None,
    bucket: str = None,
    file_prefix: str = None,
    checksum_prefix: str = None,
    kms_key_id: str = None,
    local_tmp_dir: str = None,
    sft_overlap_pool: List[str] = None,
    overlap_ratio: float = None
) -> int:
    num_files = num_files or int(os.getenv("NUM_GP_FILES", 20))
    valid_per_file = valid_per_file or int(os.getenv("VALID_NHS_PER_FILE", 2000))
    invalid_per_file = invalid_per_file or int(os.getenv("INVALID_NHS_PER_FILE", 300))
    bucket = bucket or os.getenv("S3_BUCKET")
    file_prefix = file_prefix or os.getenv("GP_FILES_PREFIX")
    checksum_prefix = checksum_prefix or os.getenv("GP_CHECKSUMS_PREFIX")
    kms_key_id = kms_key_id or os.getenv("KMS_KEY_ID")
    local_tmp_dir = local_tmp_dir or os.getenv("LOCAL_TMP_DIR", ".")
    overlap_ratio = overlap_ratio if overlap_ratio is not None else float(os.getenv("GP_SFT_OVERLAP_RATIO", 0.4))

    print(f"Generating {num_files} GP file(s)...")
    print(f"Valid NHS per file: {valid_per_file}")
    print(f"Invalid NHS per file: {invalid_per_file}")
    if sft_overlap_pool:
        print(f"   Overlap ratio with SFT: {overlap_ratio:.0%}")

    total_valid = 0

    for i in range(num_files):
        valid_nhs = []

        # Create overlap with SFT if pool provided
        if sft_overlap_pool and overlap_ratio > 0:
            overlap_count = max(1, int(valid_per_file * overlap_ratio))
            if len(sft_overlap_pool) >= overlap_count:
                valid_nhs = random.sample(sft_overlap_pool, overlap_count)

        # Fill remaining with new valid NHS numbers
        remaining = valid_per_file - len(valid_nhs)
        valid_nhs.extend([generate_valid_nhs_number() for _ in range(remaining)])
        invalid_nhs = [generate_invalid_nhs_number() for _ in range(invalid_per_file)]
        nhs_numbers = valid_nhs + invalid_nhs
        random.shuffle(nhs_numbers)

        file_path = create_csv_file(nhs_numbers, local_tmp_dir, prefix="gp")
        upload_file_and_checksum(file_path, bucket, file_prefix, checksum_prefix, kms_key_id)

        total_valid += len(valid_nhs)
        print(f"[{i+1}/{num_files}] Uploaded GP file with {len(nhs_numbers)} NHS numbers")

    return total_valid


def generate_and_upload_all_test_data(
    num_gp_files: int = None,
    num_sft_files: int = None,
    valid_per_file: int = None,
    invalid_per_file: int = None,
    overlap_ratio: float = None
) -> None:
    sft_valid_pool, sft_count = generate_and_upload_sft_data(
        num_files=num_sft_files,
        valid_per_file=valid_per_file,
        invalid_per_file=invalid_per_file
    )

    gp_count = generate_and_upload_gp_data(
        num_files=num_gp_files,
        valid_per_file=valid_per_file,
        invalid_per_file=invalid_per_file,
        sft_overlap_pool=sft_valid_pool,
        overlap_ratio=overlap_ratio
    )

    print("Test data generation complete!")
    print(f"SFT files: {num_sft_files or os.getenv('NUM_SFT_FILES')} (valid NHS: {sft_count})")
    print(f"GP files: {num_gp_files or os.getenv('NUM_GP_FILES')} (valid NHS: {gp_count})")
    print()


if __name__ == "__main__":
    # Option 1: Generate both SFT and GP data
    generate_and_upload_all_test_data()

    # Option 2: Generate only SFT data
    # generate_and_upload_sft_data()

    # Option 3: Generate only GP data (without SFT overlap)
    # generate_and_upload_gp_data()
