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

- AES-SIV (RFC 5297) - Two-pass, no nonce needed
- AES-GCM-SIV (RFC 8452) - Single-pass, nonce required but misuse-resistant

### AES-SIV (Synthetic IV) - How it Works

Two-Pass Construction:

```
Pass 1: SIV = CMAC(Key1, Data || Associated_Data)  → Generate IV
Pass 2: Ciphertext = AES-CTR(Key2, Data, IV=SIV)   → Encrypt data
```

In simple terms:

1. Create a fingerprint: Takes your data and creates a unique "fingerprint" (CMAC) that identifies it
2. Use fingerprint as lock code: This fingerprint becomes the lock combination for encrypting
3. Encrypt the data: Uses standard AES encryption with the fingerprint as the key
4. Package together: Combines the fingerprint and encrypted data into one result

Why this works for NHS data:

- Same NHS number always gets same fingerprint → same encrypted result (good for joining datasets)
- No need to remember or generate random numbers (nonces)
- If someone tampers with the data, the fingerprint won't match (tamper detection)

### AES-GCM-SIV (Galois/Counter Mode with SIV) - How it Works

Single-Pass with Nonce:

```
Key_Auth, Key_Enc = Derive_Keys(Key, Nonce)
Tag = POLYVAL(Key_Auth, Data || Associated_Data || Lengths)
IV = AES(Key_Enc, Tag ⊕ Nonce)
Ciphertext = AES-CTR(Key_Enc, Data, IV)
```

In simple terms:

1. Split the key: Takes your main key and splits it into two specialized keys (one for checking, one for encrypting)
2. Create a checksum: Makes a mathematical summary (POLYVAL) of the data for verification
3. Mix with random number: Combines the checksum with a random number (nonce) to create encryption settings
4. Encrypt in one go: Encrypts everything in a single, fast operation

Why this works for NHS data:

- Much faster than AES-SIV (88% faster in our tests)
- Still deterministic when you reuse the same random number
- Built-in tamper detection like AES-SIV
- Good for high-volume NHS processing

## Key Differences Summary:

| Aspect                | AES-SIV                          | AES-GCM-SIV                      | Winner     |
|-----------------------|----------------------------------|----------------------------------|------------|
| Performance           | 41,117 records/sec               | 77,350 records/sec (88% faster)  | GCM-SIV    |
| Nonce Handling        | None required                    | Required (but safe if reused)    | SIV        |
| Cross-Source Matching | Automatic (no nonce)             | Requires deterministic nonce     | SIV        |
| Associated Data       | Must be identical across systems | Must be identical across systems | Equal      |
| Implementation        | Simpler                          | More complex nonce management    | SIV        |
| Library Support       | Google Tink, cryptography        | cryptography (OpenSSL ≥3.2)      | SIV        |
| Security & Standards  | RFC 5297, proven in production   | RFC 8452, AWS adopted            | Equivalent |

## Test Results (100K NHS Records):

Both algorithms passed all security tests:

- 100% deterministic encryption (100k NHS numbers)
- 100% tamper detection (10 test samples)
- Zero collisions in 1,000 unique NHS pseudonyms
- Nonce misuse resistance: 1,000 unique pseudonyms even with same nonce (GCM-SIV)
- Cross-dataset joinability: 100% successful joins on NHS pseudonyms

Cross-Source Matching Results:

- AES-GCM-SIV: Different nonces create different pseudonyms for same NHS number → no matching possible unless
  deterministic nonce strategy used
- AES-SIV: Same NHS number always creates identical pseudonyms → Natural cross-source matching

Realistic Performance comparison (5 fields per record):

- AES-GCM-SIV: 77,350 records/sec, 386,749 fields/sec (1.29s total)
- AES-SIV: 41,117 records/sec, 205,584 fields/sec (2.43s total)
- Performance difference**: AES-GCM-SIV is 88% faster
- Total operations: 500,000 pseudonyms (100k records × 5 sensitive fields)

## Architecture Considerations:

### AES-SIV (RFC 5297)

- Pros: No nonce management, simpler implementation, wider library support
- Cons: 88% slower performance due to two-pass construction
- Best for: Maximum implementation simplicity, broad compatibility

### AES-GCM-SIV (RFC 8452)

- Pros: 88% faster performance, single-pass efficiency
- Cons: Requires nonce generation strategy (counter/random)
- Best for: High-performance NHS-scale processing, serverless environments

## Implementation Notes:

Critical for Cross-Source Consistency:
- Associated Data Strategy: Use standardized associated data across all systems to ensure deterministic matching:
  - Algorithm identifier: `b"aes-gcm-siv"` or `b"aes-siv"`
  - Key version: `b"key-v1"` or `b"key-v2"` 
  - Field type: `b"nhs_number"`, `b"postcode"`, `b"name"`, etc.
  - Example: `[b"aes-gcm-siv", b"key-v1", b"nhs_number"]`

For AES-GCM-SIV Deterministic Cross-Source Matching:
- Nonce Strategy: Derive nonce from data itself (not random/counter)
- Implementation: `nonce = SHA256(nhs_number + field_type + key_version)[:12]`
- Critical: All systems must use identical nonce derivation logic

For AES-SIV:
- Associated Data: Must be identical across all systems
- No special nonce considerations - naturally deterministic

Both Algorithms:
- Implement key rotation with version tracking in associated data
- Use consistent field type identifiers across all pipeline components
- Document and enforce associated data standards organization-wide

## Recommendation:

AES-GCM-SIV is strongly recommended for NHS-scale workloads based on 88% performance advantage while maintaining
equivalent security.

Key benefits verified through testing:

- Significant performance gain (77K vs 41K records/sec)
- 88% faster when processing realistic NHS workloads (5 fields per patient)
- Lower processing costs in serverless environments
- Proven security with graceful nonce misuse degradation
- Supported in Python cryptography ecosystem

Alternative: AES-SIV remains viable if eliminating nonce management complexity is the top priority.
