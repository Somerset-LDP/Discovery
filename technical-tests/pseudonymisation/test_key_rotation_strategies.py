import hashlib
import logging

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV

from logging_setup import setup_test_logging


@pytest.fixture(scope="module")
def setup_logging_fixture():
    setup_test_logging("key_rotation_strategies")


@pytest.fixture
def sample_nhs_data():
    return ["1234567890", "2345678901", "3456789012"]


def create_deterministic_nonce(data: str, key_version: str, field_type: str) -> bytes:
    nonce_input = f"{data}{key_version}{field_type}"
    hash_obj = hashes.Hash(hashes.SHA256(), backend=default_backend())
    hash_obj.update(nonce_input.encode())
    return hash_obj.finalize()[:12]


def encrypt_with_key(data: str, key: bytes, key_version: str, field_type: str) -> str:
    nonce = create_deterministic_nonce(data, key_version, field_type)
    associated_data = f"{key_version}|{field_type}".encode()
    pseudonym = AESGCMSIV(key).encrypt(nonce, data.encode(), associated_data)
    return pseudonym.hex()


def create_dataset_pseudonyms(nhs_data: list, key: bytes, key_version: str) -> dict:
    return {nhs: encrypt_with_key(nhs, key, key_version, "nhs_number")
            for nhs in nhs_data}


def count_joinable_records(dataset1: dict, dataset2: dict) -> int:
    return sum(1 for nhs in dataset1
               if dataset1[nhs] == dataset2[nhs])


# =============================================================================
# STRATEGY 1: Single Long-Lived Master Key
# =============================================================================

def test_single_key_perfect_joins_total_vulnerability(setup_logging_fixture, sample_nhs_data):
    logging.info("=== Testing Single Long-Lived Key Strategy ===")

    master_key = b'0' * 32

    gp_pseudonyms = create_dataset_pseudonyms(sample_nhs_data, master_key, "master")
    hospital_pseudonyms = create_dataset_pseudonyms(sample_nhs_data, master_key, "master")

    joinable = count_joinable_records(gp_pseudonyms, hospital_pseudonyms)

    logging.info(f"Joinability: {joinable}/{len(sample_nhs_data)} (100% - perfect)")

    assert joinable == len(sample_nhs_data), "Perfect joins with single key"


# =============================================================================
# STRATEGY 2: Key Versioning with Re-encryption  
# =============================================================================

def test_key_versioning_synchronization_requirement(setup_logging_fixture, sample_nhs_data):
    logging.info("=== Testing Key Versioning Strategy ===")

    key_v1 = b'1' * 32
    key_v2 = b'2' * 32

    # Same version works
    gp_v1 = create_dataset_pseudonyms(sample_nhs_data, key_v1, "key-v1")
    hospital_v1 = create_dataset_pseudonyms(sample_nhs_data, key_v1, "key-v1")
    joins_same_version = count_joinable_records(gp_v1, hospital_v1)

    # Different versions fail completely
    gp_v1 = create_dataset_pseudonyms(sample_nhs_data, key_v1, "key-v1")
    hospital_v2 = create_dataset_pseudonyms(sample_nhs_data, key_v2, "key-v2")
    joins_different_versions = count_joinable_records(gp_v1, hospital_v2)

    logging.info(f"Same version joins: {joins_same_version}/{len(sample_nhs_data)}")
    logging.info(f"Cross-version joins: {joins_different_versions}/{len(sample_nhs_data)}")

    assert joins_same_version == len(sample_nhs_data), "Joins work within same version"
    assert joins_different_versions == 0, "Joins fail across versions"


# =============================================================================
# STRATEGY 3: Two-Layer Hash + Encrypted Pseudonym (Recommended)
# =============================================================================

def test_two_layer_permanent_joins_independent_security(setup_logging_fixture, sample_nhs_data):
    logging.info("=== Testing Two-Layer Strategy (Recommended) ===")

    master_salt = "nhs_master_salt_v1"
    gp_key = b'g' * 32
    hospital_key = b'h' * 32

    master_ids = {nhs: hashlib.sha256(f"{nhs}{master_salt}".encode()).hexdigest()
                  for nhs in sample_nhs_data}

    gp_data = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, gp_key, "gp-key-v1", "nhs_number")
    } for nhs in sample_nhs_data}

    hospital_data = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, hospital_key, "hospital-key-v1", "nhs_number")
    } for nhs in sample_nhs_data}

    joinable_via_master_id = sum(1 for nhs in sample_nhs_data
                                 if gp_data[nhs]['master_id'] == hospital_data[nhs]['master_id'])

    different_encryptions = sum(1 for nhs in sample_nhs_data
                                if gp_data[nhs]['encrypted_pseudonym'] != hospital_data[nhs]['encrypted_pseudonym'])

    logging.info(f"Joinable via master_id: {joinable_via_master_id}/{len(sample_nhs_data)}")
    logging.info(f"Different encrypted pseudonyms: {different_encryptions}/{len(sample_nhs_data)}")

    assert joinable_via_master_id == len(sample_nhs_data), "Perfect joins via master ID"
    assert different_encryptions == len(sample_nhs_data), "Different encryptions for security"


def test_two_layer_independent_key_rotation(setup_logging_fixture, sample_nhs_data):
    logging.info("Testing independent key rotation")

    master_salt = "nhs_master_salt_v1"
    gp_key_v1 = b'g' * 32
    gp_key_v2 = b'G' * 32
    hospital_key = b'h' * 32

    master_ids = {nhs: hashlib.sha256(f"{nhs}{master_salt}".encode()).hexdigest()
                  for nhs in sample_nhs_data}

    # BEFORE rotation - both systems use original keys
    gp_before_rotation = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, gp_key_v1, "gp-key-v1", "nhs_number")
    } for nhs in sample_nhs_data}

    hospital_before_rotation = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, hospital_key, "hospital-key-v1", "nhs_number")
    } for nhs in sample_nhs_data}

    joins_before_rotation = sum(1 for nhs in sample_nhs_data
                                if gp_before_rotation[nhs]['master_id'] == hospital_before_rotation[nhs]['master_id'])

    # AFTER rotation - GP rotates key, Hospital keeps same key
    gp_after_rotation = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, gp_key_v2, "gp-key-v2", "nhs_number")
    } for nhs in sample_nhs_data}

    hospital_unchanged = {nhs: {
        'master_id': master_ids[nhs],
        'encrypted_pseudonym': encrypt_with_key(nhs, hospital_key, "hospital-key-v1", "nhs_number")
    } for nhs in sample_nhs_data}

    joins_after_rotation = sum(1 for nhs in sample_nhs_data
                               if gp_after_rotation[nhs]['master_id'] == hospital_unchanged[nhs]['master_id'])

    logging.info(f"Joins before rotation: {joins_before_rotation}/{len(sample_nhs_data)}")
    logging.info(f"Joins after rotation: {joins_after_rotation}/{len(sample_nhs_data)}")

    assert joins_before_rotation == len(sample_nhs_data), "Joins must work before rotation"
    assert joins_after_rotation == len(sample_nhs_data), "Joins preserved after rotation"
    assert joins_before_rotation == joins_after_rotation, "Rotation doesn't affect joinability"
