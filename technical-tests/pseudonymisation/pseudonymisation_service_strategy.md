# Pseudonymisation Service Strategy

*Strategic and Tactical Solutions for Somerset LDP*

## Executive Summary

Somerset LDP requires pseudonymisation that enables secure cross-dataset joins while protecting NHS data. We recommend
local pseudonymisation for cost-effectiveness and performance, with remote re-identification for controlled analytical
access, supplemented by additional services for cross-dataset linking and external data access.

Key Decision: Deterministic joinability is critical for NHS analytics while keeping sensitive identifiers within LDP
infrastructure.

---

## Tactical Solution: Lambda Service

Discovery Prototype:  For discovery purposes, all functionalities are combined into a single service prototype with
simplified functionality to demonstrate core capabilities.

### Single Lambda with Endpoints

1. Encryption Endpoint

```
POST /encrypt
Input: raw_data, dataset_key
Output: encrypted_pseudonym
```

2. Re-identification Endpoint

```
POST /decrypt
Input: encrypted_pseudonym, dataset_key
Output: raw_data
```

### Service Characteristics

- Stateless Functions: Auto-scaling based on demand
- Key Management: Cloud KMS integration
- Security: No sensitive data exposure

---

## Strategic Solution: Hybrid Architecture

The core approach combines local pseudonymisation for high-volume data processing with remote re-identification for controlled analytical access.

Local Pseudonymisation Library:

- Purpose: High-performance, cost-effective data encryption during ingestion
- Operations: `raw_data → encrypted_pseudonym`
- Benefits: No network calls, independent scaling, minimal operational costs
- Use case: High-volume data processing pipelines

Remote Re-identification Service:

- Purpose: Controlled decryption for authorized analytical access
- Operations: `encrypted_pseudonym → raw_data` (with authorization)
- Benefits: Centralized access control, audit logging, governance
- Use case: Analyst requests for data interpretation

Rationale for Split Architecture:

- Cost optimization: Local pseudonymisation eliminates network overhead for bulk operations
- Performance: No latency for high-volume data ingestion
- Security: Re-identification centralized with role-based access control
- Operational simplicity: Independent scaling of ingestion vs. analytical workloads

---

## Technical Decisions

Core technical choices balance security, performance, and operational requirements for NHS-scale data processing.

### Algorithm Choice

Selected: Deterministic authenticated encryption, AES-SIV (RFC 5297)
Rationale:

- Better performance: Despite two-stage process, no nonce generation required makes it faster than AES-GCM-SIV
- Security and transparency prioritized
- Easy process management and reversibility
- Deterministic output for consistent operations
- Strong protection against key misuse

### Key Type

Selected: 256-bit symmetric encryption keys
Rationale:

- AES-256 provides strong encryption for data protection standards
- Symmetric keys enable high-performance encryption/decryption operations
- Suitable for deterministic encryption requirements
- Industry standard for healthcare data protection
- Asymmetric keys considered: Would be overcomplicated given AES algorithm characteristics and performance requirements

### Key Management & Rotation Strategy

Selected: Cloud-managed keys with periodic rotation per dataset
Rationale:
- Independent dataset security: Each dataset uses separate encryption keys, limiting blast radius of potential compromise
- Operational efficiency: Cloud KMS provides automated key generation, secure storage, and rotation capabilities
- Zero-downtime rotation: New data encrypted with new keys while existing data remains accessible with old keys
- Cost optimization: Pay-per-use model scales with actual encryption operations
- Audit capabilities: Complete key usage logging

Implementation:
- Encryption Keys: Cloud KMS per dataset with configurable rotation periods
- Key Versioning: Multiple key versions maintained to support historical data access
- Access Control: IAM policies restrict key usage to authorized services only
- Emergency Procedures: Immediate key revocation capability for compromise scenarios

### Architecture Choice

Selected: Split architecture separating encryption and decryption operations
Rationale:
- Cost efficiency: Local pseudonymisation eliminates network overhead for high-volume data ingestion
- Performance optimization: No network latency during bulk data processing operations
- Security centralization: Re-identification controlled through centralized service with authorization
- Operational separation: Independent scaling of data ingestion vs. analytical access workloads
- Risk management: Bulk processing continues during analytical service outages

## Technical Documentation

Detailed
Analysis: [GitHub Repository](https://github.com/Somerset-LDP/Discovery/tree/main/technical-tests/pseudonymisation)

- `algorithm_evaluation.md` - AES-SIV selection rationale
- `key_evaluation.md` - Key management approach analysis
- `key_rotation_strategy.md` - Two-layer key management approach
- `remote_vs_local_evaluation.md` - Hybrid architecture analysis

---
