# Additional Services Strategy

## Overview

Beyond core pseudonymisation, the Somerset LDP platform requires additional services to enable cross-dataset analytics while maintaining security and compliance. These decisions address long-term operational requirements for analytical access, data linking, and external integrations.

---

## Cross-Dataset Linking Strategy

### Master ID Generation Service

Decision: Implement deterministic master identifier generation for permanent cross-dataset joins

Implementation:
- Generate stable identifiers: `master_id = HMAC_SHA256(master_key, nhs_number)`
- Store master keys in Secure Key Storage (KMS, HMS)
- Minimal rotation - only when security requires

Rationale:
- Enables consistent linking across multiple datasets
- Maintains referential integrity without exposing NHS numbers
- Preserves joins during dataset key rotations
- HSM storage provides maximum security for critical linking capability

---

## Analytical Access Strategy

### Remote Analysis Service

Decision: Centralized analytical access with role-based permissions and comprehensive governance

Functionality:
- Process complex queries across datasets using master_ids
- Role-based access control for different data categories (standard, special category, re-identification)
- Comprehensive audit logging of all access attempts
- Rate limiting and approval workflows for high-privilege operations

Benefits:
- Centralized security controls and monitoring
- Consistent access patterns across all analytical use cases
- Complete audit trail for compliance requirements
- Granular permission management per user/role

---

## External Access Strategy

### Token Management Service

Decision: Protect internal identifiers through temporary token substitution

Implementation:
- Replace master_ids with temporary random tokens in external API responses
- Time-limited tokens (15-minute expiry) with no relationship to internal identifiers
- Token-to-identifier mapping maintained only during active sessions

Security Benefits:
- Internal master_ids never leave LDP infrastructure
- External data breaches cannot compromise internal linking capability
- Temporary nature limits exposure window
- No reverse-engineering of internal identifier structure possible

---

## Integration Architecture

### Service Coordination Flow

Long-term Integration Pattern:

1. Data Ingestion: Raw data → Local pseudonymisation → Storage with master_id
2. Analytical Access: Query → Analysis service matches via master_ids → Returns data with temporary tokens  
3. Re-identification: Authorized requests → Remote service validation → Decrypted data delivery

Operational Benefits:
- Clear separation of concerns between services
- Independent scaling and deployment of each component
- Fault isolation - failure in one service doesn't affect others
- Technology flexibility - services can evolve independently

---

## Technical Documentation

Core Pseudonymisation Strategy: [Pseudonymisation Service Strategy](pseudonymisation_service_strategy.md)

Supporting Analysis: [GitHub Repository](https://github.com/Somerset-LDP/Discovery/tree/main/technical-tests/pseudonymisation)
- `key_rotation_strategy.md` - Master ID and key management approach
- `remote_vs_local_evaluation.md` - Service architecture analysis

---
