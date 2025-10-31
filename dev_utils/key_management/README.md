# Key Management Utilities

Utilities for generating and managing encryption keys for the pseudonymisation service.

## Module

### `generate_encrypted_key.py`
Generates AES-256 encryption keys and encrypts them with AWS KMS for secure storage.

**Purpose:** Creates 256-bit (32-byte) data encryption keys (DEKs) that are encrypted with a KMS Customer Master Key (CMK) for the pseudonymisation Lambda function.

**Note:** The script generates AES-256 keys (256 bits). For AES-SIV, which requires 512-bit keys, two AES-256 keys can be concatenated, or the key derivation can be handled within the pseudonymisation service.

## Usage

```bash
cd dev_utils/key_management
python generate_encrypted_key.py --kms-key-id <KMS_KEY_ID>
```

**With version label:**
```bash
python generate_encrypted_key.py --kms-key-id arn:aws:kms:eu-west-2:123456:key/abc --version v2
```

**Add to existing key structure:**
```bash
python generate_encrypted_key.py --kms-key-id alias/pseudo-key --version v2 --add-to-existing
```

The script will:
1. Generate a 256-bit (32-byte) AES key using KMS
2. Encrypt it using the specified KMS CMK
3. Output the base64-encoded encrypted key

The encrypted key should be stored in AWS Secrets Manager and referenced in the pseudonymisation Lambda configuration.


