# Pseudonymisation Lambda - Direct Invocation

This Lambda is designed for **direct invocation** from other Lambda functions.
It processes **one field at a time** (single value or list of values).

## Event Format

```json
{
  "action": "encrypt",           // Required: "encrypt" or "reidentify"
  "field_name": "nhs_number",    // Required: name of the field (used in AAD)
  "field_value": "1234567890",   // Required: string or list of strings
  "correlation_id": "abc-123"    // Optional: for tracing
}
```

## Examples

### 1. Encrypt single value

**Input:**
```json
{
  "action": "encrypt",
  "field_name": "nhs_number",
  "field_value": "1234567890",
  "correlation_id": "req-001"
}
```

**Output:**
```json
{
  "field_name": "nhs_number",
  "field_value": "ZXhhbXBsZV9lbmNyeXB0ZWRfZGF0YQ=="
}
```

### 2. Encrypt list of values

**Input:**
```json
{
  "action": "encrypt",
  "field_name": "mrn",
  "field_value": ["MRN001", "MRN002", "MRN003"],
  "correlation_id": "req-002"
}
```

**Output:**
```json
{
  "field_name": "mrn",
  "field_value": [
    "YWJjZGVmZ2hpamtsbW5vcA==",
    "cXJzdHV2d3h5ejEyMzQ1Ng==",
    "Nzg5MGFiY2RlZmdoaWprbA=="
  ]
}
```

### 3. Reidentify single value

**Input:**
```json
{
  "action": "reidentify",
  "field_name": "nhs_number",
  "field_value": "ZXhhbXBsZV9lbmNyeXB0ZWRfZGF0YQ==",
  "correlation_id": "req-003"
}
```

**Output:**
```json
{
  "field_name": "nhs_number",
  "field_value": "1234567890"
}
```

### 4. Reidentify list of values

**Input:**
```json
{
  "action": "reidentify",
  "field_name": "mrn",
  "field_value": [
    "YWJjZGVmZ2hpamtsbW5vcA==",
    "cXJzdHV2d3h5ejEyMzQ1Ng=="
  ],
  "correlation_id": "req-004"
}
```

**Output:**
```json
{
  "field_name": "mrn",
  "field_value": ["MRN001", "MRN002"]
}
```

## Error Response

When an error occurs, the response contains a meaningful error message:

```json
{
  "error": "Value cannot be empty",
  "correlation_id": "req-005"
}
```

### Common Error Messages

**Validation errors:**
- `'action' is required (encrypt or reidentify)`
- `'field_name' is required`
- `'field_value' is required`
- `Invalid action: {action}. Must be 'encrypt' or 'reidentify'`
- `Value cannot be empty`
- `Pseudonym cannot be empty`

**Configuration errors:**
- `Environment variable SECRET_NAME_KMS_KEY not set`
- `Environment variable SECRET_NAME_KEY_VERSIONS not set`
- `Environment variable ALGORITHM_ID not set`
- `Secret name cannot be empty`
- `Key versions secret missing 'current' field`

**AWS service errors:**
- `AWS service error: AccessDeniedException` - Missing permissions
- `AWS service error: ResourceNotFoundException` - Secret or KMS key not found
- `AWS service error: InvalidCiphertextException` - Decryption failed (wrong key/data)

**Encryption/decryption errors:**
- `Encryption/decryption failed: {specific_error}` - Cryptographic operation failed

## Invoking from Another Lambda

```python
import boto3
import json

lambda_client = boto3.client('lambda')

# Encrypt a single value
response = lambda_client.invoke(
    FunctionName='pseudonymisation-dat-processing',
    InvocationType='RequestResponse',
    Payload=json.dumps({
        'action': 'encrypt',
        'field_name': 'nhs_number',
        'field_value': '1234567890',
        'correlation_id': 'my-correlation-id'
    })
)

result = json.loads(response['Payload'].read())
encrypted_value = result['field_value']

# Encrypt a list of values
response = lambda_client.invoke(
    FunctionName='pseudonymisation-lambda',
    InvocationType='RequestResponse',
    Payload=json.dumps({
        'action': 'encrypt',
        'field_name': 'mrn',
        'field_value': ['MRN001', 'MRN002', 'MRN003'],
        'correlation_id': 'my-correlation-id'
    })
)

result = json.loads(response['Payload'].read())
encrypted_list = result['field_value']

# Reidentify
response = lambda_client.invoke(
    FunctionName='pseudonymisation-lambda',
    InvocationType='RequestResponse',
    Payload=json.dumps({
        'action': 'reidentify',
        'field_name': 'nhs_number',
        'field_value': encrypted_value,
        'correlation_id': 'my-correlation-id'
    })
)

result = json.loads(response['Payload'].read())
original_value = result['field_value']
```

## Environment Variables

Required:

- `SECRET_NAME_KMS_KEY` - Name of AWS Secrets Manager secret containing KMS key ID `arn:aws:kms:region:value`
- `SECRET_NAME_KEY_VERSIONS` - Name of secret containing key versions JSON: `{"current": "v1", "keys": {"v1": "base64-encoded-key"}}`
- `ALGORITHM_ID` - Algorithm identifier (e.g., "AES-SIV")

## Key Generation Pattern

This implementation uses **Envelope Encryption with Manual Data Key Rotation**. Data keys are generated once using KMS
`generate_data_key`, then stored in encrypted form in Secrets Manager. When needed, these encrypted data keys are
decrypted via KMS `decrypt`, ensuring deterministic encryption - the same plaintext data key is always returned for a
given version.

To generate new encrypted data keys, use the helper script (this can be automated for future project phases):

```bash
python dev_utils/generate_encrypted_key.py --kms-key-id <KMS_ARN> --version v1
python dev_utils/generate_encrypted_key.py --kms-key-id alias/pseudonymisation-key --version v2 --add-to-existing
```

## Key Features

- **Single field processing** - One field at a time, simplifies caller logic
- **List support** - Automatically handles both single values and lists
- **Correlation tracking** - Pass through correlation_id for distributed tracing
- **Simple interface** - No HTTP concepts, just pure data transformation
- **Direct invocation** - Optimized for Lambda-to-Lambda calls, no API Gateway overhead
