import logging
import os

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESSIV

from dev_utils.pseudonymisation.logging_setup import setup_test_logging


@pytest.fixture(scope="module")
def setup_logging_fixture():
    setup_test_logging("aessiv_constraints")
    logging.info("=== AES-SIV Size Requirements Test ===")


def test_key_sizes(setup_logging_fixture):
    logging.info("Starting key size tests")
    test_data = b"test_data"
    associated_data = b"nhs_pseudonymisation"

    valid_key_sizes = [32, 48, 64]  # 256, 384, 512 bits
    for size in valid_key_sizes:
        key = os.urandom(size)
        cipher = AESSIV(key)
        encrypted = cipher.encrypt(test_data, [associated_data])
        decrypted = cipher.decrypt(encrypted, [associated_data])
        assert decrypted == test_data
        logging.info(f"Key size {size} bytes ({size * 8} bits): success")

    invalid_key_sizes = [16, 24, 40]  # 128, 192, 320 bits
    for size in invalid_key_sizes:
        with pytest.raises(Exception):
            key = os.urandom(size)
            cipher = AESSIV(key)
            cipher.encrypt(test_data, [associated_data])
        logging.info(f"Key size {size} bytes ({size * 8} bits): failed as expected")
