import pytest
import logging
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV
import os
from dev_utils.pseudonymisation.logging_setup import setup_test_logging


@pytest.fixture(scope="module")
def setup_logging_fixture():
    setup_test_logging("aesgcmsiv_constraints")
    logging.info("=== AES-GCM-SIV Size Requirements Test ===")


def test_key_sizes(setup_logging_fixture):
    logging.info("Starting key size tests")
    test_data = b"test_data"
    test_nonce = os.urandom(12)

    valid_key_sizes = [16, 24, 32]  # 128, 192, 256 bits
    for size in valid_key_sizes:
        key = os.urandom(size)
        cipher = AESGCMSIV(key)
        encrypted = cipher.encrypt(test_nonce, test_data, None)
        decrypted = cipher.decrypt(test_nonce, encrypted, None)
        assert decrypted == test_data
        logging.info(f"Key size {size} bytes ({size*8} bits): success")

    invalid_key_sizes = [40]  # 320 bits
    for size in invalid_key_sizes:
        with pytest.raises(Exception):
            key = os.urandom(size)
            cipher = AESGCMSIV(key)
        logging.info(f"Key size {size} bytes ({size*8} bits): failed as expected")


def test_nonce_sizes(setup_logging_fixture):
    logging.info("Starting nonce size tests")
    test_data = b"test_data"
    key = os.urandom(32)
    cipher = AESGCMSIV(key)

    valid_nonce = os.urandom(12)
    encrypted = cipher.encrypt(valid_nonce, test_data, None)
    decrypted = cipher.decrypt(valid_nonce, encrypted, None)
    assert decrypted == test_data
    logging.info("Nonce size 12 bytes (96 bits): success")

    invalid_nonce_sizes = [8, 16, 20]
    for size in invalid_nonce_sizes:
        with pytest.raises(Exception):
            nonce = os.urandom(size)
            cipher.encrypt(nonce, test_data, None)
        logging.info(f"Nonce size {size} bytes ({size*8} bits): failed as expected")
