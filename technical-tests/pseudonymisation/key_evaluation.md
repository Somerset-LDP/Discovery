# Symmetric vs Asymmetric Key Choice

## Context:

- Centralized LDP platform performs all pseudonymisation internally
- External providers send unencrypted data to LDP
- LDP encrypts before storage, decrypts on client request
- Clients receive already decrypted data
- Deterministic encryption required for cross-dataset joins

## Key Finding: Why Asymmetric is Unnecessarily Complex

Critical Point: AES algorithms (both AES-GCM-SIV and AES-SIV) are inherently symmetric - they require the same key for both encryption and decryption. This fundamental constraint makes asymmetric key management pointless for our use case.

The Reality of "Asymmetric" Approach:
- RSA public key → HKDF derivation → 32-byte AES symmetric key
- Same derived AES key used for both encrypt and decrypt operations
- No actual separation of encrypt/decrypt permissions
- Cannot use RSA keys directly with AES - must always derive symmetric key first

Result: Asymmetric approach = symmetric encryption with unnecessary complexity layer

## Test Results

### Performance Testing

Both approaches use identical AES-GCM-SIV algorithm - only key management differs:

| Approach              | Encrypt Performance | Decrypt Performance | Key Size           |
|-----------------------|---------------------|---------------------|--------------------|
| Symmetric             | 97,553 ops/sec      | Similar             | 32 bytes           |
| Asymmetric Derivation | 265,849 ops/sec     | Similar             | 294 bytes (public) |

Performance difference: Negligible (6.5ms for 1000 operations)

### Algorithm Constraints

- AES-GCM-SIV: Requires exactly 12-byte nonce, accepts 16/24/32-byte keys
- AES-SIV: No nonce required, requires minimum 32-byte keys

## Recommendation

Use Symmetric Key Management (AWS KMS/Azure Key Vault)

1. Simplicity: Direct AES key storage and retrieval
2. Standard Pattern: Standard approach used by AWS KMS/Azure Key Vault for centralized encryption
4. KMS Integration: Built-in key rotation, access controls, audit logging

## Implementation Notes:

- Store 256-bit AES key in AWS KMS/Azure Key Vault
- Service-specific IAM roles for key access
- Standard KMS audit and rotation procedures
