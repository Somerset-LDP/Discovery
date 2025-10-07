# Symmetric vs Asymmetric Key Choice

## Context:

- Centralized LDP platform performs all pseudonymisation internally
- External providers send unencrypted data to LDP
- LDP encrypts before storage, decrypts on client request
- Clients receive already decrypted data
- Deterministic encryption required for cross-dataset joins

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

### Key Finding: Asymmetric Approach Issues

- RSA public key → HKDF → 32-byte AES key
- Same derived AES key used for both encrypt and decrypt
- No separation of encrypt/decrypt permissions
- Cannot use RSA public/private keys directly with AES algorithms, because AES requires a symmetric key (same key for
  encrypt/decrypt), while RSA provides asymmetric key pairs (different keys for different operations)

Asymmetric approach = symmetric encryption with unnecessary complexity

## Recommendation

Use Symmetric Key Management (AWS KMS/Azure Key Vault)

1. Simplicity: Direct AES key storage and retrieval
2. Standard Pattern: Standard approach used by AWS KMS/Azure Key Vault for centralized encryption
3. No False Benefits: Asymmetric approach provides no security advantage
4. KMS Integration: Built-in key rotation, access controls, audit logging

## Implementation Notes:

- Store 256-bit AES key in AWS KMS/Azure Key Vault
- Use AES-GCM-SIV with deterministic nonce strategy
- Service-specific IAM roles for key access
- Standard KMS audit and rotation procedures
