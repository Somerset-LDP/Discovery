import csv
import hashlib
import os
import random
from datetime import datetime

import boto3
from dev_utils.synthetic_data.data_generators import generate_valid_nhs_number, generate_invalid_nhs_number


def generate_nhs_csv_file(valid_count: int, invalid_count: int, output_dir: str = '.') -> str:
    nhs_numbers = [generate_valid_nhs_number() for _ in range(valid_count)]
    nhs_numbers += [generate_invalid_nhs_number() for _ in range(invalid_count)]
    random.shuffle(nhs_numbers)
    filename = f"nhs_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{random.randint(1000, 9999)}.csv"
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
        f.write(checksum + '\n')
    return checksum_file


def upload_to_s3(file_path: str, bucket: str, key: str, kms_key_id: str) -> None:
    s3 = boto3.client('s3')
    with open(file_path, 'rb') as f:
        s3.upload_fileobj(
            f,
            bucket,
            key,
            ExtraArgs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": kms_key_id
            }
        )


def generate_and_upload_nhs_test_data(
        num_gp_files: int,
        num_sft_files: int,
        valid_per_file: int,
        invalid_per_file: int,
        gp_bucket: str,
        gp_prefix: str,
        sft_bucket: str,
        sft_prefix: str,
        checksum_gp_prefix: str,
        checksum_sft_prefix: str,
        local_tmp_dir: str = ".",
        gp_sft_overlap_ratio: float = 0.4
) -> None:
    sft_valid_numbers = [generate_valid_nhs_number() for _ in range(valid_per_file * num_sft_files)]
    sft_invalid_numbers = [generate_invalid_nhs_number() for _ in range(invalid_per_file * num_sft_files)]
    sft_idx = 0
    for i in range(num_sft_files):
        sft_nums = sft_valid_numbers[sft_idx:sft_idx + valid_per_file] + sft_invalid_numbers[
                                                                         sft_idx:sft_idx + invalid_per_file]
        random.shuffle(sft_nums)
        sft_idx += valid_per_file
        filename = f"nhs_sft_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{random.randint(1000, 9999)}.csv"
        file_path = os.path.join(local_tmp_dir, filename)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for nhs in sft_nums:
                writer.writerow([nhs])
        sft_filename = os.path.basename(file_path)
        sft_key = os.path.join(sft_prefix, sft_filename)
        upload_to_s3(file_path, sft_bucket, sft_key, kms_key_id)
        sft_checksum = generate_sha256_checksum(file_path)
        sft_checksum_filename = os.path.basename(sft_checksum)
        sft_checksum_key = os.path.join(checksum_sft_prefix, sft_checksum_filename)
        upload_to_s3(sft_checksum, sft_bucket, sft_checksum_key, kms_key_id)
        os.remove(file_path)
        os.remove(sft_checksum)
    sft_valid_pool = sft_valid_numbers.copy()
    for i in range(num_gp_files):
        overlap_count = max(1, int(valid_per_file * gp_sft_overlap_ratio))
        gp_valid = random.sample(sft_valid_pool, overlap_count) if len(
            sft_valid_pool) >= overlap_count else sft_valid_pool.copy()
        gp_valid += [generate_valid_nhs_number() for _ in range(valid_per_file - len(gp_valid))]
        gp_invalid = [generate_invalid_nhs_number() for _ in range(invalid_per_file)]
        nhs_numbers = gp_valid + gp_invalid
        random.shuffle(nhs_numbers)
        filename = f"nhs_gp_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{random.randint(1000, 9999)}.csv"
        file_path = os.path.join(local_tmp_dir, filename)
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for nhs in nhs_numbers:
                writer.writerow([nhs])
        gp_filename = os.path.basename(file_path)
        gp_key = os.path.join(gp_prefix, gp_filename)
        upload_to_s3(file_path, gp_bucket, gp_key, kms_key_id)
        gp_checksum = generate_sha256_checksum(file_path)
        gp_checksum_filename = os.path.basename(gp_checksum)
        gp_checksum_key = os.path.join(checksum_gp_prefix, gp_checksum_filename)
        upload_to_s3(gp_checksum, gp_bucket, gp_checksum_key, kms_key_id)
        os.remove(file_path)
        os.remove(gp_checksum)


if __name__ == "__main__":
    gp_bucket = "somerset-647582858282-dev-cohort-upload"
    gp_prefix = "uploads/registered-patient/files/"
    sft_bucket = "somerset-647582858282-dev-cohort-upload"
    sft_prefix = "uploads/overnight-stay/files/"
    checksum_gp_prefix = "uploads/registered-patient/checksums/"
    checksum_sft_prefix = "uploads/overnight-stay/checksums/"
    kms_key_id = os.getenv("KMS_KEY_ID")

    generate_and_upload_nhs_test_data(
        num_gp_files=20,
        num_sft_files=1,
        valid_per_file=2000,
        invalid_per_file=300,
        gp_bucket=gp_bucket,
        gp_prefix=gp_prefix,
        sft_bucket=sft_bucket,
        sft_prefix=sft_prefix,
        checksum_gp_prefix=checksum_gp_prefix,
        checksum_sft_prefix=checksum_sft_prefix,
        local_tmp_dir=""
    )
