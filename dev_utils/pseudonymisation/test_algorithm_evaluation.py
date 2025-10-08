import logging
import os
import time

import pandas as pd
import psutil
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV, AESSIV

from dev_utils.synthetic_data.data_generators import (
    generate_valid_nhs_number,
    generate_random_name,
    generate_random_surname,
    generate_random_dob,
    generate_random_postcode
)
from dev_utils.pseudonymisation.logging_setup import setup_test_logging


@pytest.fixture(scope="module")
def test_data():
    setup_test_logging("algorithm_evaluation")
    logging.info("Generating 100k synthetic NHS records...")
    key = b"0" * 32  # 256-bit key
    n = 100000  # 100k rows for performance
    df = pd.DataFrame({
        "nhs_number": [generate_valid_nhs_number() for _ in range(n)],
        "name": [generate_random_name() for _ in range(n)],
        "surname": [generate_random_surname() for _ in range(n)],
        "dob": [generate_random_dob() for _ in range(n)],
        "postcode": [generate_random_postcode() for _ in range(n)]
    })
    logging.info(f"Generated {len(df)} records successfully")
    return key, df


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


def pseudonymise_field(algorithm, field_data, field_name, associated_data):
    if isinstance(algorithm, AESGCMSIV):
        base_nonce = b"field_" + field_name.encode()
        if len(base_nonce) >= 12:
            nonce = base_nonce[:12]
        else:
            nonce = base_nonce.ljust(12, b'0')

        return [algorithm.encrypt(nonce, value.encode(), associated_data) for value in field_data]
    else:  # AESSIV
        if isinstance(associated_data, list):
            ad = associated_data + [field_name.encode()]
        else:
            ad = [associated_data, field_name.encode()]
        return [algorithm.encrypt(value.encode(), ad) for value in field_data]


# ========================================
# DETERMINISM TESTS - NHS NUMBER FIELD
# Test if same NHS number produces same pseudonym (essential for joins)
# ========================================

def test_determinism_nhs_field_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: NHS Field Determinism Test (100k records) ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    nhs_pseudonyms_1 = pseudonymise_field(aesgcmsiv, df['nhs_number'], 'nhs_number', associated_data=associated_data)
    nhs_pseudonyms_2 = pseudonymise_field(aesgcmsiv, df['nhs_number'], 'nhs_number', associated_data=associated_data)

    log_resource_usage(start, "AESGCMSIV NHS determinism", pre_cpu)
    mismatches = sum(a != b for a, b in zip(nhs_pseudonyms_1, nhs_pseudonyms_2))
    message = f"AESGCMSIV NHS determinism mismatches: {mismatches}/100000"
    print(message)
    logging.info(message)
    assert mismatches == 0


def test_determinism_nhs_field_aessiv(test_data):
    logging.info("=== AESSIV: NHS Field Determinism Test (100k records) ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation"]

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    nhs_pseudonyms_1 = pseudonymise_field(aessiv, df['nhs_number'], 'nhs_number', associated_data=associated_data)
    nhs_pseudonyms_2 = pseudonymise_field(aessiv, df['nhs_number'], 'nhs_number', associated_data=associated_data)

    log_resource_usage(start, "AESSIV NHS determinism", pre_cpu)
    mismatches = sum(a != b for a, b in zip(nhs_pseudonyms_1, nhs_pseudonyms_2))
    message = f"AESSIV NHS determinism mismatches: {mismatches}/100000"
    print(message)
    logging.info(message)
    assert mismatches == 0


# ========================================
# PERFORMANCE TESTS - FIELD LEVEL
# Test performance of pseudonymising all sensitive fields per record
# scenario: for each patient record, pseudonymise NHS, name, surname, dob, postcode
# ========================================

def test_performance_realistic_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Realistic Performance Test (100k records × 5 fields) ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    total_pseudonyms = 0
    sensitive_fields = ['nhs_number', 'name', 'surname', 'dob', 'postcode']

    for field in sensitive_fields:
        field_pseudonyms = pseudonymise_field(aesgcmsiv, df[field], field, associated_data=associated_data)
        total_pseudonyms += len(field_pseudonyms)

    log_resource_usage(start, "AESGCMSIV realistic performance", pre_cpu)

    total_time = time.time() - start
    records_per_sec = len(df) / total_time
    fields_per_sec = total_pseudonyms / total_time

    message = f"AESGCMSIV realistic throughput: {records_per_sec:.2f} records/sec, {fields_per_sec:.2f} fields/sec"
    message += f" (100k records × 5 fields = {total_pseudonyms:,} total pseudonyms)"
    print(message)
    logging.info(message)


def test_performance_realistic_aessiv(test_data):
    logging.info("=== AESSIV: Realistic Performance Test (100k records × 5 fields) ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation"]

    process = psutil.Process(os.getpid())
    pre_cpu = process.cpu_percent(interval=None)
    start = time.time()

    total_pseudonyms = 0
    sensitive_fields = ['nhs_number', 'name', 'surname', 'dob', 'postcode']

    for field in sensitive_fields:
        field_pseudonyms = pseudonymise_field(aessiv, df[field], field, associated_data=associated_data)
        total_pseudonyms += len(field_pseudonyms)

    log_resource_usage(start, "AESSIV realistic performance", pre_cpu)

    total_time = time.time() - start
    records_per_sec = len(df) / total_time
    fields_per_sec = total_pseudonyms / total_time

    message = f"AESSIV realistic throughput: {records_per_sec:.2f} records/sec, {fields_per_sec:.2f} fields/sec"
    message += f" (100k records × 5 fields = {total_pseudonyms:,} total pseudonyms)"
    print(message)
    logging.info(message)


# ========================================
# CROSS-DATASET JOIN DEMONSTRATION
# Show practical joining using field pseudonyms
# ========================================

def test_cross_dataset_join_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Cross-Dataset Join Test ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"
    sample_df = df.head(100).copy()
    nhs_pseudonyms = pseudonymise_field(aesgcmsiv, sample_df['nhs_number'], 'nhs_number',
                                        associated_data=associated_data)
    sample_df['nhs_pseudonym'] = [ct.hex() for ct in nhs_pseudonyms]

    gp_data = sample_df[['nhs_pseudonym', 'name']].copy()
    hospital_data = sample_df[['nhs_pseudonym', 'dob']].copy()

    joined = gp_data.merge(hospital_data, on='nhs_pseudonym', how='inner')

    message = f"AESGCMSIV cross-dataset join: {len(joined)} records successfully joined on NHS pseudonym"
    print(message)
    logging.info(message)
    assert len(joined) == 100, "All records should be joinable"


def test_cross_dataset_join_aessiv(test_data):
    logging.info("=== AESSIV: Cross-Dataset Join Test ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation"]

    sample_df = df.head(100).copy()

    nhs_pseudonyms = pseudonymise_field(aessiv, sample_df['nhs_number'], 'nhs_number', associated_data=associated_data)
    sample_df['nhs_pseudonym'] = [ct.hex() for ct in nhs_pseudonyms]

    gp_data = sample_df[['nhs_pseudonym', 'name']].copy()
    hospital_data = sample_df[['nhs_pseudonym', 'dob']].copy()

    joined = gp_data.merge(hospital_data, on='nhs_pseudonym', how='inner')

    message = f"AESSIV cross-dataset join: {len(joined)} records successfully joined on NHS pseudonym"
    print(message)
    logging.info(message)
    assert len(joined) == 100, "All records should be joinable"


# ========================================
# CROSS-SOURCE MATCHING TESTS
# Verify pseudonym matching for same NHS number from different sources
# ========================================

def test_cross_source_matching_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Cross-Source Matching Test ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"
    same_nhs = "1234567890"

    nonce_source1 = b"source1_0001"
    pseudonym_source1 = aesgcmsiv.encrypt(nonce_source1, same_nhs.encode(), associated_data)

    nonce_source2 = b"source2_0001"
    pseudonym_source2 = aesgcmsiv.encrypt(nonce_source2, same_nhs.encode(), associated_data)

    different_pseudonyms = pseudonym_source1 != pseudonym_source2

    decrypted1 = aesgcmsiv.decrypt(nonce_source1, pseudonym_source1, associated_data)
    decrypted2 = aesgcmsiv.decrypt(nonce_source2, pseudonym_source2, associated_data)
    same_original = decrypted1 == decrypted2 == same_nhs.encode()

    message = f"AESGCMSIV cross-source: Different nonces create different pseudonyms: {different_pseudonyms}"
    message += f", but decrypt to same NHS: {same_original}"
    print(message)
    logging.info(message)

    assert different_pseudonyms, "Same NHS with different nonces should create different pseudonyms"
    assert same_original, "Both should decrypt to same original NHS number"

    # Additional test: we want deterministic matching across sources
    deterministic_nonce = hashes.Hash(hashes.SHA256(), backend=default_backend())
    deterministic_nonce.update(same_nhs.encode())
    det_nonce_bytes = deterministic_nonce.finalize()[:12]

    pseudonym_det1 = aesgcmsiv.encrypt(det_nonce_bytes, same_nhs.encode(), associated_data)
    pseudonym_det2 = aesgcmsiv.encrypt(det_nonce_bytes, same_nhs.encode(), associated_data)

    deterministic_match = pseudonym_det1 == pseudonym_det2
    message = f"AESGCMSIV with deterministic nonce: Same NHS creates same pseudonym: {deterministic_match}"
    print(message)
    logging.info(message)

    assert deterministic_match, "Deterministic nonce should create identical pseudonyms"


def test_cross_source_matching_aessiv(test_data):
    logging.info("=== AESSIV: Cross-Source Matching Test ===")
    key, df = test_data
    aessiv = AESSIV(key)

    associated_data = [b"nhs_pseudonymisation"]
    same_nhs = "1234567890"

    pseudonym_source1 = aessiv.encrypt(same_nhs.encode(), associated_data)
    pseudonym_source2 = aessiv.encrypt(same_nhs.encode(), associated_data)

    same_pseudonyms = pseudonym_source1 == pseudonym_source2

    decrypted1 = aessiv.decrypt(pseudonym_source1, associated_data)
    decrypted2 = aessiv.decrypt(pseudonym_source2, associated_data)
    same_original = decrypted1 == decrypted2 == same_nhs.encode()

    message = f"AESSIV cross-source: Same NHS creates identical pseudonyms: {same_pseudonyms}"
    message += f", and decrypts to same NHS: {same_original}"
    print(message)
    logging.info(message)

    assert same_pseudonyms, "AES-SIV should create identical pseudonyms for same NHS"
    assert same_original, "Both should decrypt to same original NHS number"

    # Test with different source-specific associated data
    associated_data_gp = [b"nhs_pseudonymisation", b"gp_source"]
    associated_data_hospital = [b"nhs_pseudonymisation", b"hospital_source"]

    pseudonym_gp = aessiv.encrypt(same_nhs.encode(), associated_data_gp)
    pseudonym_hospital = aessiv.encrypt(same_nhs.encode(), associated_data_hospital)

    different_with_source_tag = pseudonym_gp != pseudonym_hospital

    message = f"AESSIV with source tags: Different associated data creates different pseudonyms: {different_with_source_tag}"
    print(message)
    logging.info(message)

    assert different_with_source_tag, "Different associated data should create different pseudonyms"


# ========================================
# TAMPER DETECTION TESTS
# Test integrity protection on pseudonymised fields
# ========================================

def test_tamper_detection_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Tamper Detection Test (NHS field) ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"

    tamper_detected = 0
    for i in range(10):
        nhs = df['nhs_number'].iloc[i]
        nonce = b"field_nhs_nu"
        ct = aesgcmsiv.encrypt(nonce, nhs.encode(), associated_data)
        tampered = ct[:-1] + bytes([ct[-1] ^ 0xFF])
        try:
            aesgcmsiv.decrypt(nonce, tampered, associated_data)
            logging.error(f"AESGCMSIV tamper detection FAILED for NHS {i}!")
            assert False
        except Exception as e:
            tamper_detected += 1
            logging.info(f"AESGCMSIV tamper detected in NHS {i}: {type(e).__name__}")

    message = f"AESGCMSIV successfully detected tampering in {tamper_detected}/10 NHS pseudonyms"
    print(message)
    logging.info(message)


def test_tamper_detection_aessiv(test_data):
    logging.info("=== AESSIV: Tamper Detection Test (NHS field) ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation", b"nhs_number"]

    tamper_detected = 0
    for i in range(10):
        nhs = df['nhs_number'].iloc[i]
        ct = aessiv.encrypt(nhs.encode(), associated_data)
        tampered = ct[:-1] + bytes([ct[-1] ^ 0xFF])
        try:
            aessiv.decrypt(tampered, associated_data)
            logging.error(f"AESSIV tamper detection FAILED for NHS {i}!")
            assert False
        except Exception as e:
            tamper_detected += 1
            logging.info(f"AESSIV tamper detected in NHS {i}: {type(e).__name__}")

    message = f"AESSIV successfully detected tampering in {tamper_detected}/10 NHS pseudonyms"
    print(message)
    logging.info(message)


# ========================================
# NONCE HANDLING TESTS
# Test nonce misuse resistance and collision behavior
# ========================================

def test_nonce_misuse_resistance_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Nonce Misuse Resistance Test (NHS field, same nonce) ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    nonce = b"same_nonce12"  # Intentionally reuse same nonce for different NHS numbers
    associated_data = b"nhs_pseudonymisation"

    nhs_sample = df['nhs_number'].head(1000)
    cts = [aesgcmsiv.encrypt(nonce, nhs.encode(), associated_data) for nhs in nhs_sample]
    unique_cts = len(set(cts))

    message = f"AESGCMSIV nonce misuse resistance: {unique_cts}/1000 unique NHS pseudonyms with same nonce"
    print(message)
    logging.info(message)
    assert unique_cts == 1000


def test_nonce_independence_aessiv(test_data):
    logging.info("=== AESSIV: Nonce Independence Test (NHS field, no nonce needed) ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation", b"nhs_number"]
    nhs_sample = df['nhs_number'].head(1000)
    cts = [aessiv.encrypt(nhs.encode(), associated_data) for nhs in nhs_sample]
    unique_cts = len(set(cts))

    message = f"AESSIV nonce independence: {unique_cts}/1000 unique NHS pseudonyms (no nonce needed)"
    print(message)
    logging.info(message)
    assert unique_cts == 1000


# ========================================
# COLLISION RESISTANCE TESTS
# Verify no hash collisions in pseudonyms
# ========================================

def test_collision_resistance_aesgcmsiv(test_data):
    logging.info("=== AESGCMSIV: Collision Resistance Test (NHS field, 1000 records) ===")
    key, df = test_data
    aesgcmsiv = AESGCMSIV(key)
    associated_data = b"nhs_pseudonymisation"

    nhs_sample = df['nhs_number'].head(1000)
    nhs_pseudonyms = pseudonymise_field(aesgcmsiv, nhs_sample, 'nhs_number', associated_data=associated_data)
    unique_pseudonyms = len(set(nhs_pseudonyms))

    message = f"AESGCMSIV collision resistance: {unique_pseudonyms}/1000 unique NHS pseudonyms"
    print(message)
    logging.info(message)
    assert unique_pseudonyms == 1000


def test_collision_resistance_aessiv(test_data):
    logging.info("=== AESSIV: Collision Resistance Test (NHS field, 1000 records) ===")
    key, df = test_data
    aessiv = AESSIV(key)
    associated_data = [b"nhs_pseudonymisation"]

    nhs_sample = df['nhs_number'].head(1000)
    nhs_pseudonyms = pseudonymise_field(aessiv, nhs_sample, 'nhs_number', associated_data=associated_data)
    unique_pseudonyms = len(set(nhs_pseudonyms))

    message = f"AESSIV collision resistance: {unique_pseudonyms}/1000 unique NHS pseudonyms"
    print(message)
    logging.info(message)
    assert unique_pseudonyms == 1000

    logging.info("=== Field-Level Pseudonymisation Test Suite Completed ===")
