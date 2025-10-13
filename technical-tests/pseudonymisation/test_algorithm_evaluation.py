import logging
import os
import threading
import time
from contextlib import contextmanager

import pandas as pd
import psutil
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV, AESSIV

from logging_setup import setup_test_logging
from dev_utils.synthetic_data.data_generators import (
    generate_valid_nhs_number,
    generate_random_name,
    generate_random_surname,
    generate_random_dob,
    generate_random_postcode
)


@contextmanager
def cpu_monitor():
    process = psutil.Process(os.getpid())

    process.cpu_percent()
    time.sleep(0.1)

    cpu_measurements = []
    monitoring = True

    def monitor_cpu():
        while monitoring:
            try:
                cpu = process.cpu_percent(interval=0.1)
                if 0 < cpu <= 100:
                    cpu_measurements.append(cpu)
            except:
                pass

    monitor_thread = threading.Thread(target=monitor_cpu, daemon=True)
    monitor_thread.start()

    try:
        yield cpu_measurements
    finally:
        monitoring = False
        monitor_thread.join(timeout=1.0)


@pytest.fixture(scope="module")
def test_data():
    setup_test_logging("algorithm_evaluation")
    key = b"0" * 32
    df = generate_test_data(100000)
    return key, df


def generate_test_data(num_records):
    df = pd.DataFrame({
        "nhs_number": [generate_valid_nhs_number() for _ in range(num_records)],
        "name": [generate_random_name() for _ in range(num_records)],
        "surname": [generate_random_surname() for _ in range(num_records)],
        "dob": [generate_random_dob() for _ in range(num_records)],
        "postcode": [generate_random_postcode() for _ in range(num_records)]
    })
    return df


def get_memory_usage():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def derive_deterministic_nonce(data: str, field_type: str) -> bytes:
    nonce_input = f"{data}{field_type}"
    hash_obj = hashes.Hash(hashes.SHA256(), backend=default_backend())
    hash_obj.update(nonce_input.encode())
    return hash_obj.finalize()[:12]


def log_resource_usage(start_time, label, pre_cpu=None):
    end_time = time.time()
    process = psutil.Process(os.getpid())

    # Get current CPU and calculate difference if we have pre_cpu
    if pre_cpu is not None:
        cpu_percent = process.cpu_percent(interval=None)
        cpu_usage = max(0, cpu_percent - pre_cpu)
    else:
        cpu_usage = process.cpu_percent(interval=0.1)

    mem_info = process.memory_info().rss / (1024 * 1024)
    message = f"{label} - Time: {end_time - start_time:.2f}s, CPU: {cpu_usage:.1f}%, RAM: {mem_info:.2f}MB"
    print(message)
    logging.info(message)


def get_associated_data(algorithm_type, field_name, key_version="v1"):
    return [
        algorithm_type.encode(),
        f"key_{key_version}".encode(),
        field_name.encode()
    ]


def pseudonymise_field(algorithm, field_data, field_name, algorithm_type, key_version="v1"):
    associated_data = get_associated_data(algorithm_type, field_name, key_version)

    if isinstance(algorithm, AESGCMSIV):
        # AES-GCM-SIV expects bytes, so join the list
        ad_bytes = b"|".join(associated_data)
        return [algorithm.encrypt(derive_deterministic_nonce(value, field_name), value.encode(), ad_bytes) for value in
                field_data]
    else:  # AESSIV
        # AES-SIV expects list directly
        return [algorithm.encrypt(value.encode(), associated_data) for value in field_data]


def create_cipher(algorithm_type, key):
    if algorithm_type == "AESGCMSIV":
        return AESGCMSIV(key)
    elif algorithm_type == "AESSIV":
        return AESSIV(key)
    else:
        raise ValueError(f"Unknown algorithm type: {algorithm_type}")


# ========================================
# DETERMINISM TESTS - NHS NUMBER FIELD
# Test if same NHS number produces same pseudonym (essential for joins)
# ========================================

def test_determinism_nhs_field_aesgcmsiv(test_data):
    run_determinism_test("AESGCMSIV", test_data)


def test_determinism_nhs_field_aessiv(test_data):
    run_determinism_test("AESSIV", test_data)


# ========================================
# PERFORMANCE TESTS - PURE ENCRYPTION
# Test pure encryption performance without operational overhead
# ========================================

def test_pure_encryption_performance_aesgcmsiv(test_data):
    run_pure_encryption_test("AESGCMSIV", test_data)


def test_pure_encryption_performance_aessiv(test_data):
    run_pure_encryption_test("AESSIV", test_data)


# ========================================
# CROSS-DATASET JOIN DEMONSTRATION
# Show practical joining using field pseudonyms
# ========================================

def test_cross_dataset_join_aesgcmsiv(test_data):
    run_join_test("AESGCMSIV", test_data)


def test_cross_dataset_join_aessiv(test_data):
    run_join_test("AESSIV", test_data)


# ========================================
# TAMPER DETECTION TESTS
# Test integrity protection on pseudonymised fields
# ========================================

def test_tamper_detection_aesgcmsiv(test_data):
    run_tamper_detection_test("AESGCMSIV", test_data)


def test_tamper_detection_aessiv(test_data):
    run_tamper_detection_test("AESSIV", test_data)


# ========================================
# COLLISION RESISTANCE TESTS
# Verify no hash collisions in pseudonyms
# ========================================

def test_collision_resistance_aesgcmsiv(test_data):
    run_collision_resistance_test("AESGCMSIV", test_data)


def test_collision_resistance_aessiv(test_data):
    run_collision_resistance_test("AESSIV", test_data)

    logging.info("=== Field-Level Pseudonymisation Test Suite Completed ===")


# ========================================
# SCALABILITY TESTS
# Test performance across different data volumes
# ========================================

def test_scalability_aesgcmsiv():
    run_scalability_test("AESGCMSIV")


def test_scalability_aessiv():
    run_scalability_test("AESSIV")


def run_determinism_test(algorithm_type, test_data):
    logging.info(f"=== {algorithm_type}: NHS Field Determinism Test (100k records) ===")
    key, df = test_data
    cipher = create_cipher(algorithm_type, key)

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    nhs_pseudonyms_1 = pseudonymise_field(cipher, df['nhs_number'], 'nhs_number', algorithm_type)
    nhs_pseudonyms_2 = pseudonymise_field(cipher, df['nhs_number'], 'nhs_number', algorithm_type)

    log_resource_usage(start, f"{algorithm_type} NHS determinism", pre_cpu)
    mismatches = sum(a != b for a, b in zip(nhs_pseudonyms_1, nhs_pseudonyms_2))
    message = f"{algorithm_type} NHS determinism mismatches: {mismatches}/100000"
    print(message)
    logging.info(message)
    assert mismatches == 0


def run_join_test(algorithm_type, test_data):
    logging.info(f"=== {algorithm_type}: Cross-Dataset Join Test ===")
    key, df = test_data
    cipher = create_cipher(algorithm_type, key)

    sample_df = df.head(100).copy()
    nhs_pseudonyms = pseudonymise_field(cipher, sample_df['nhs_number'], 'nhs_number', algorithm_type)
    sample_df['nhs_pseudonym'] = [ct.hex() for ct in nhs_pseudonyms]

    gp_data = sample_df[['nhs_pseudonym', 'name']].copy()
    hospital_data = sample_df[['nhs_pseudonym', 'dob']].copy()

    joined = gp_data.merge(hospital_data, on='nhs_pseudonym', how='inner')

    message = f"{algorithm_type} cross-dataset join: {len(joined)} records successfully joined on NHS pseudonym"
    print(message)
    logging.info(message)
    assert len(joined) == 100, "All records should be joinable"


def run_tamper_detection_test(algorithm_type, test_data):
    logging.info(f"=== {algorithm_type}: Tamper Detection Test (NHS field) ===")
    key, df = test_data
    cipher = create_cipher(algorithm_type, key)
    associated_data = get_associated_data(algorithm_type, "nhs_number")

    tamper_detected = 0
    for i in range(10):
        nhs = df['nhs_number'].iloc[i]

        if algorithm_type == "AESGCMSIV":
            ad_bytes = b"|".join(associated_data)
            nonce = derive_deterministic_nonce(nhs, "nhs_number")
            ct = cipher.encrypt(nonce, nhs.encode(), ad_bytes)
        else:  # AESSIV
            ct = cipher.encrypt(nhs.encode(), associated_data)

        tampered = ct[:-1] + bytes([ct[-1] ^ 0xFF])

        try:
            if algorithm_type == "AESGCMSIV":
                cipher.decrypt(nonce, tampered, ad_bytes)
            else:  # AESSIV
                cipher.decrypt(tampered, associated_data)

            logging.error(f"{algorithm_type} tamper detection FAILED for NHS {i}!")
            assert False
        except Exception as e:
            tamper_detected += 1
            logging.info(f"{algorithm_type} tamper detected in NHS {i}: {type(e).__name__}")

    message = f"{algorithm_type} successfully detected tampering in {tamper_detected}/10 NHS pseudonyms"
    print(message)
    logging.info(message)


def run_collision_resistance_test(algorithm_type, test_data):
    logging.info(f"=== {algorithm_type}: Collision Resistance Test (NHS field, 1000 records) ===")
    key, df = test_data
    cipher = create_cipher(algorithm_type, key)

    nhs_sample = df['nhs_number'].head(1000)
    nhs_pseudonyms = pseudonymise_field(cipher, nhs_sample, 'nhs_number', algorithm_type)
    unique_pseudonyms = len(set(nhs_pseudonyms))

    message = f"{algorithm_type} collision resistance: {unique_pseudonyms}/1000 unique NHS pseudonyms"
    print(message)
    logging.info(message)
    assert unique_pseudonyms == 1000


def run_scalability_test(algorithm_type):
    logging.info(f"=== {algorithm_type} Scalability Test ===")

    key = b'0' * 32
    cipher = create_cipher(algorithm_type, key)
    test_volumes = [1000, 5000, 10000, 25000, 50000, 100000, 500000, 1000000]
    results = []

    for volume in test_volumes:
        df = generate_test_data(volume)
        baseline_memory = get_memory_usage()
        peak_memory = baseline_memory

        with cpu_monitor() as cpu_measurements:
            start_time = time.perf_counter()
            total_operations = 0

            for field_name in ['nhs_number', 'name', 'surname', 'dob', 'postcode']:
                associated_data = get_associated_data(algorithm_type, field_name)
                for value in df[field_name]:
                    if algorithm_type == "AESGCMSIV":
                        ad_bytes = b"|".join(associated_data)
                        nonce = derive_deterministic_nonce(value, field_name)
                        cipher.encrypt(nonce, value.encode(), ad_bytes)
                    else:  # AESSIV
                        cipher.encrypt(value.encode(), associated_data)
                    total_operations += 1

                if total_operations % 5000 == 0:
                    current_memory = get_memory_usage()
                    peak_memory = max(peak_memory, current_memory)

            end_time = time.perf_counter()

        final_memory = get_memory_usage()
        duration = end_time - start_time
        ops_per_sec = total_operations / duration if duration > 0 else 0
        memory_growth = final_memory - baseline_memory

        if cpu_measurements:
            avg_cpu_usage = sum(cpu_measurements) / len(cpu_measurements)
        else:
            avg_cpu_usage = psutil.cpu_percent(interval=0.1)

        result = {
            'volume': volume,
            'total_operations': total_operations,
            'duration': duration,
            'ops_per_sec': ops_per_sec,
            'baseline_memory_mb': baseline_memory,
            'peak_memory_mb': peak_memory,
            'final_memory_mb': final_memory,
            'memory_growth_mb': memory_growth,
            'cpu_percent': avg_cpu_usage,
            'cpu_samples': len(cpu_measurements)
        }
        results.append(result)

    logging.info(f"=== {algorithm_type} Performance Summary ===")
    for result in results:
        logging.info(f"{result['volume']:>8,} records: {result['ops_per_sec']:>8,.0f} ops/sec, "
                     f"{result['duration']:>6.2f}s, CPU: {result['cpu_percent']:>5.1f}%")


def run_pure_encryption_test(algorithm_type, test_data):
    logging.info(f"=== {algorithm_type}: Pure Encryption Performance Test (100k records × 5 fields) ===")
    key, df = test_data
    cipher = create_cipher(algorithm_type, key)

    # Pre-create all data and parameters before timing
    sensitive_fields = ['nhs_number', 'name', 'surname', 'dob', 'postcode']
    prepared_data = []

    for field in sensitive_fields:
        associated_data = get_associated_data(algorithm_type, field, "v1")

        for value in df[field]:
            if algorithm_type == "AESGCMSIV":
                ad_bytes = b"|".join(associated_data)
                nonce = derive_deterministic_nonce(value, field)
                prepared_data.append((nonce, value.encode(), ad_bytes))
            else:  # AESSIV
                prepared_data.append((value.encode(), associated_data))

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    # Time only the encrypt operations
    total_encryptions = 0
    for data in prepared_data:
        if algorithm_type == "AESGCMSIV":
            nonce, plaintext, ad_bytes = data
            cipher.encrypt(nonce, plaintext, ad_bytes)
        else:  # AESSIV
            plaintext, associated_data = data
            cipher.encrypt(plaintext, associated_data)
        total_encryptions += 1

    log_resource_usage(start, f"{algorithm_type} pure encryption", pre_cpu)

    total_time = time.time() - start
    encryptions_per_sec = total_encryptions / total_time
    records_per_sec = len(df) / total_time

    message = f"{algorithm_type} pure encryption throughput: {encryptions_per_sec:.2f} encryptions/sec, {records_per_sec:.2f} complete records/sec"
    message += f" (100k records × 5 fields = {total_encryptions:,} pure encryptions)"
    print(message)
    logging.info(message)
