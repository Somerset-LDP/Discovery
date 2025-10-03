# Encryption Algorithm Choice

## Context

Selection of an encryption algorithm for deterministic pseudonymization of NHS data (NHS numbers, postcodes, names,
DOB). Deterministic behaviour enables joins across datasets but leaks equality (same input → same ciphertext).

## Requirements:

- Secure, standards-based algorithm
- Deterministic encryption for joinability
- High performance for NHS-scale workloads
- Operational maintainability and future-proofing

## Candidate Algorithms:

- **AES-SIV** (RFC 5297) - Two-pass, no nonce needed
- **AES-GCM-SIV** (RFC 8452) - Single-pass, nonce required but misuse-resistant

## Key Differences Summary:

| Aspect                   | AES-SIV                          | AES-GCM-SIV                         | Winner     |
|--------------------------|----------------------------------|-------------------------------------|------------|
| **Performance**          | 41,117 records/sec               | 77,350 records/sec (**88% faster**) | GCM-SIV    |
| **Nonce Handling**       | ✅ None required                  | ⚠️ Required (but safe if reused)    | SIV        |
| **Implementation**       | ✅ Simpler                        | More complex nonce management       | SIV        |
| **Library Support**      | Google Tink, cryptography        | cryptography (OpenSSL ≥3.2)         | SIV        |
| **Security & Standards** | ✅ RFC 5297, proven in production | ✅ RFC 8452, AWS adopted             | Equivalent |

## Test Results (100K NHS Records):

**Both algorithms passed all security tests:**

- 100% deterministic encryption (100k NHS numbers)
- 100% tamper detection (10 test samples)
- Zero collisions in 1,000 unique NHS pseudonyms
- Nonce misuse resistance: 1,000 unique pseudonyms even with same nonce (GCM-SIV)
- Cross-dataset joinability: 100% successful joins on NHS pseudonyms

**Realistic Performance comparison (5 fields per record):**

- **AES-GCM-SIV**: 77,350 records/sec, 386,749 fields/sec (1.29s total)
- **AES-SIV**: 41,117 records/sec, 205,584 fields/sec (2.43s total)
- **Performance difference**: AES-GCM-SIV is **88% faster**
- **Total operations**: 500,000 pseudonyms (100k records × 5 sensitive fields)

## Architecture Considerations:

### AES-SIV (RFC 5297)

- **Pros**: No nonce management, simpler implementation, wider library support
- **Cons**: 88% slower performance due to two-pass construction
- **Best for**: Maximum implementation simplicity, broad compatibility

### AES-GCM-SIV (RFC 8452)

- **Pros**: 88% faster performance, single-pass efficiency
- **Cons**: Requires nonce generation strategy (counter/random)
- **Best for**: High-performance NHS-scale processing, serverless environments

## Implementation Notes:

- **For AES-GCM-SIV**: Use counter-based or high-entropy random nonces
- **For AES-SIV**: No special considerations needed
- **Both**: Implement key rotation and context separation for multi-tenant use

## Recommendation:

**AES-GCM-SIV is strongly recommended** for NHS-scale workloads based on 88% performance advantage while maintaining
equivalent security.

**Key benefits verified through testing:**

- Significant performance gain (77K vs 41K records/sec)
- 88% faster when processing realistic NHS workloads (5 fields per patient)
- Lower processing costs in serverless environments
- Proven security with graceful nonce misuse degradation
- Supported in Python cryptography ecosystem

**Alternative:** AES-SIV remains viable if eliminating nonce management complexity is the top priority.
