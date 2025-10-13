import logging
import os
import time

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from logging_setup import setup_test_logging


@pytest.fixture(scope="module")
def setup_logging_fixture():
    setup_test_logging("key_management")
    logging.info("=== AES-GCM-SIV Key Management Comparison ===")


@pytest.fixture(scope="module")
def test_data():
    nhs_number = b'1234567890'
    nonce = hashes.Hash(hashes.SHA256(), backend=default_backend())
    nonce.update(nhs_number)
    nonce_bytes = nonce.finalize()[:12]
    associated_data = b'nhs_pseudonymisation'
    return nhs_number, nonce_bytes, associated_data


def test_symmetric_key_performance(setup_logging_fixture, test_data):
    logging.info("Starting symmetric key approach test")
    nhs_number, nonce_bytes, associated_data = test_data

    mock_kms_key = os.urandom(32)
    logging.info(f"KMS key size: {len(mock_kms_key)} bytes")
    aesgcmsiv_symmetric = AESGCMSIV(mock_kms_key)

    start = time.time()
    for i in range(1000):
        ct = aesgcmsiv_symmetric.encrypt(nonce_bytes, nhs_number, associated_data)
    sym_encrypt_time = time.time() - start

    start = time.time()
    for i in range(1000):
        pt = aesgcmsiv_symmetric.decrypt(nonce_bytes, ct, associated_data)
    sym_decrypt_time = time.time() - start

    encrypt_perf = 1000 / sym_encrypt_time
    decrypt_perf = 1000 / sym_decrypt_time
    logging.info(f"Symmetric - Encrypt performance: {encrypt_perf:.0f} ops/sec")
    logging.info(f"Symmetric - Decrypt performance: {decrypt_perf:.0f} ops/sec")
    logging.info(f"Symmetric - Decrypted data matches: {pt == nhs_number}")

    assert pt == nhs_number
    return sym_encrypt_time, sym_decrypt_time


def test_asymmetric_key_derivation_performance(setup_logging_fixture, test_data):
    logging.info("Starting asymmetric key derivation approach test")
    nhs_number, nonce_bytes, associated_data = test_data

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    derived_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'nhs_pseudonymisation_salt',
        info=b'aes_key_derivation',
        backend=default_backend()
    ).derive(public_key_bytes)

    logging.info(f"RSA public key size: {len(public_key_bytes)} bytes")
    logging.info(f"Derived AES key size: {len(derived_key)} bytes")

    aesgcmsiv_derived = AESGCMSIV(derived_key)

    start = time.time()
    for i in range(1000):
        ct_derived = aesgcmsiv_derived.encrypt(nonce_bytes, nhs_number, associated_data)
    asym_encrypt_time = time.time() - start

    start = time.time()
    for i in range(1000):
        pt_derived = aesgcmsiv_derived.decrypt(nonce_bytes, ct_derived, associated_data)
    asym_decrypt_time = time.time() - start

    asym_encrypt_perf = 1000 / asym_encrypt_time
    asym_decrypt_perf = 1000 / asym_decrypt_time
    logging.info(f"Asymmetric - Encrypt performance: {asym_encrypt_perf:.0f} ops/sec")
    logging.info(f"Asymmetric - Decrypt performance: {asym_decrypt_perf:.0f} ops/sec")
    logging.info(f"Asymmetric - Decrypted data matches: {pt_derived == nhs_number}")

    assert pt_derived == nhs_number
    return asym_encrypt_time, asym_decrypt_time


def test_determinism(setup_logging_fixture, test_data):
    nhs_number, nonce_bytes, associated_data = test_data

    # Symmetric approach
    mock_kms_key = os.urandom(32)
    aesgcmsiv_symmetric = AESGCMSIV(mock_kms_key)

    # Asymmetric approach
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    derived_key = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'nhs_pseudonymisation_salt',
        info=b'aes_key_derivation',
        backend=default_backend()
    ).derive(public_key_bytes)
    aesgcmsiv_derived = AESGCMSIV(derived_key)

    ct_sym_1 = aesgcmsiv_symmetric.encrypt(nonce_bytes, nhs_number, associated_data)
    ct_sym_2 = aesgcmsiv_symmetric.encrypt(nonce_bytes, nhs_number, associated_data)
    ct_der_1 = aesgcmsiv_derived.encrypt(nonce_bytes, nhs_number, associated_data)
    ct_der_2 = aesgcmsiv_derived.encrypt(nonce_bytes, nhs_number, associated_data)

    sym_deterministic = ct_sym_1 == ct_sym_2
    der_deterministic = ct_der_1 == ct_der_2

    logging.info(f"Symmetric deterministic: {sym_deterministic}")
    logging.info(f"Derived deterministic: {der_deterministic}")

    assert sym_deterministic
    assert der_deterministic


def test_same_derived_key_usage(setup_logging_fixture, test_data):
    nhs_number, nonce_bytes, associated_data = test_data

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    derived_key_1 = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'nhs_pseudonymisation_salt',
        info=b'aes_key_derivation',
        backend=default_backend()
    ).derive(public_key_bytes)

    derived_key_2 = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'nhs_pseudonymisation_salt',
        info=b'aes_key_derivation',
        backend=default_backend()
    ).derive(public_key_bytes)

    assert derived_key_1 == derived_key_2

    aesgcmsiv_1 = AESGCMSIV(derived_key_1)
    aesgcmsiv_2 = AESGCMSIV(derived_key_2)

    encrypted = aesgcmsiv_1.encrypt(nonce_bytes, nhs_number, associated_data)
    decrypted = aesgcmsiv_2.decrypt(nonce_bytes, encrypted, associated_data)

    result = decrypted == nhs_number
    assert result
    logging.info("Same derived key used for both encrypt and decrypt operations, result: " + str(result))
