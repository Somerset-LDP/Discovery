# Pseudonymisation Lambda

Deterministic encryption service for pseudonymising sensitive healthcare identifiers.
Designed for **direct invocation** from other Lambda functions.

## Key Properties

| Property          | Description                                                                              |
|-------------------|------------------------------------------------------------------------------------------|
| **Deterministic** | Same input + same field → same pseudonym (enables joins across datasets)                 |
| **Field-bound**   | Same value in different fields → different pseudonyms (prevents cross-field correlation) |
| **Reversible**    | Authorised re-identification via `reidentify` action                                     |
| **Key versioned** | Supports key rotation with automatic version detection during decryption                 |

### Why Determinism Matters

```
encrypt("1234567890", "nhs_number") → "XYZ789ABC..."
encrypt("1234567890", "nhs_number") → "XYZ789ABC..."  ← identical (enables matching)
encrypt("1234567890", "mrn")        → "DEF456GHI..."  ← different (field-bound)
```

## Event Format

```json
{
  "action": "encrypt",
  // Required: "encrypt" or "reidentify"
  "field_name": "nhs_number",
  // Required: field identifier (part of AAD)
  "field_value": "1234567890",
  // Required: string or list of strings
  "correlation_id": "abc-123"
  // Optional: for distributed tracing
}
```

### Field Name (AAD)

The `field_name` is used as Additional Authenticated Data (AAD) in AES-SIV encryption.
This binds the pseudonym to the specific field, preventing:

- Cross-field correlation attacks
- Pseudonym reuse across different data types

**Use consistent field names** across your pipeline (e.g., always `nhs_number`, not sometimes `nhs` or `NHS_Number`).

## Examples

### Encrypt single value

**Request:**

```json
{
  "action": "encrypt",
  "field_name": "nhs_number",
  "field_value": "1234567890"
}
```

**Response:**

```json
{
  "field_name": "nhs_number",
  "field_value": "ZXhhbXBsZV9lbmNyeXB0ZWRfZGF0YQ=="
}
```

### Encrypt batch (recommended for performance)

**Request:**

```json
{
  "action": "encrypt",
  "field_name": "nhs_number",
  "field_value": [
    "1234567890",
    "0987654321",
    "5555555555"
  ]
}
```

**Response:**

```json
{
  "field_name": "nhs_number",
  "field_value": [
    "YWJjZGVmZ2hpamtsbW5vcA==",
    "cXJzdHV2d3h5ejEyMzQ1Ng==",
    "Nzg5MGFiY2RlZmdoaWprbA=="
  ]
}
```

### Reidentify (decrypt)

**Request:**

```json
{
  "action": "reidentify",
  "field_name": "nhs_number",
  "field_value": "YWJjZGVmZ2hpamtsbW5vcA=="
}
```

**Response:**

```json
{
  "field_name": "nhs_number",
  "field_value": "1234567890"
}
```

## Error Handling

**Error response format:**

```json
{
  "error": "Value cannot be empty",
  "correlation_id": "req-005"
}
```

**Common errors:**

| Error                                                    | Cause                                 |
|----------------------------------------------------------|---------------------------------------|
| `Missing required event fields: action, field_name`      | Incomplete request                    |
| `Invalid action: xxx. Must be 'encrypt' or 'reidentify'` | Wrong action value                    |
| `Value cannot be empty`                                  | Empty string or whitespace            |
| `Failed to decrypt with any available key version`       | Invalid pseudonym or wrong field_name |
| `AWS service error: AccessDeniedException`               | Missing IAM permissions               |

## Invoking from Another Lambda

```python
import boto3
import json

lambda_client = boto3.client('lambda')


def pseudonymise(field_name: str, values: list[str]) -> list[str]:
    response = lambda_client.invoke(
        FunctionName='pseudonymisation-lambda',
        InvocationType='RequestResponse',
        Payload=json.dumps({
            'action': 'encrypt',
            'field_name': field_name,
            'field_value': values
        })
    )
    result = json.loads(response['Payload'].read())

    if 'error' in result:
        raise ValueError(result['error'])

    return result['field_value']


# Usage
nhs_pseudonyms = pseudonymise('nhs_number', ['1234567890', '0987654321'])
```

## Environment Variables

| Variable                   | Description                                       |
|----------------------------|---------------------------------------------------|
| `SECRET_NAME_KMS_KEY`      | Secrets Manager name containing KMS key ARN       |
| `SECRET_NAME_KEY_VERSIONS` | Secrets Manager name containing key versions JSON |
| `ALGORITHM_ID`             | Algorithm identifier (e.g., `aes-siv`)            |
| `CACHE_TTL_HOURS`          | Data key cache TTL in hours (default: 1)          |

### Key Versions Secret Format

```json
{
  "current": "v1",
  "keys": {
    "v1": "base64-encoded-encrypted-data-key"
  }
}
```

## Key Rotation

The service supports seamless key rotation:

1. **Encryption** always uses `current` version
2. **Decryption** tries `current` first, then falls back to older versions
3. Data encrypted with old keys remains decryptable

To rotate keys:

```bash
# Generate new key version
python dev_utils/key_management/generate_encrypted_key.py \
    --kms-key-id alias/pseudonymisation-key \
    --version v2 \
    --add-to-existing

# Update SECRET_NAME_KEY_VERSIONS in Secrets Manager:
# {"current": "v2", "keys": {"v1": "...", "v2": "..."}}
```

**Envelope Encryption**: Data keys are pre-generated, encrypted with KMS, and stored in Secrets Manager.
At runtime, KMS decrypts the data key (deterministic operation), which is then used for AES-SIV encryption.
