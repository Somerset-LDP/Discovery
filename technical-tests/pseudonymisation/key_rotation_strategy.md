# Key Rotation Strategy for Deterministic Pseudonymisation

## Problem Statement

Core Challenge: Deterministic encryption ties ciphertext directly to key version. When keys rotate, pseudonyms change,
breaking cross-dataset joins unless mitigated.

NHS Context:

- All encryption/decryption happens within NHS infrastructure (no external key distribution)
- Cross-dataset joins required
- Key compromise requires immediate revocation and mitigation

## Evaluated Options

### Option 1: Single Long-Lived Master Key

Approach: One encryption key for all datasets, never rotated.

*Pseudo-code: pseudonym = encrypt(master_key, nhs_number)*

Evaluation:

| Criteria                    | Assessment | Notes                                                 |
|-----------------------------|------------|-------------------------------------------------------|
| Security & Compliance       | High Risk  | Key compromise affects all historical and future data |
| Determinism & Joinability   | Perfect    | Joins always work across all datasets                 |
| Performance & Scalability   | Excellent  | No re-encryption costs                                |
| Operational Maintainability | Simple     | Zero key management complexity                        |
| Resilience & Availability   | Poor       | Single point of failure, no rotation capability       |
| Auditability & Governance   | Limited    | No key version tracking                               |
| Future-Proofing             | Poor       | Algorithm migration requires full system rebuild      |

### Option 2: Key Versioning with Batch Re-encryption

Approach: Periodic key rotation with complete dataset re-encryption.

*Process: Decrypt all records -> Re-encrypt with new key -> Update all systems*

Evaluation:

| Criteria                    | Assessment | Notes                                             |
|-----------------------------|------------|---------------------------------------------------|
| Security & Compliance       | Good       | Regular rotation, compromise limited to timeframe |
| Determinism & Joinability   | Limited    | Joins only work within same key version           |
| Performance & Scalability   | Poor       | Full dataset re-encryption expensive              |
| Operational Maintainability | Complex    | Coordinated deployment across all systems         |
| Resilience & Availability   | Poor       | Downtime required during re-encryption            |
| Auditability & Governance   | Good       | Clear key version audit trail                     |
| Future-Proofing             | Moderate   | Algorithm migration requires full re-encryption   |

### Option 3: Hash-based Master ID + Encrypted Pseudonym

Approach: Non-reversible hash for joins + encrypted pseudonyms with rotatable keys.

*Structure:*
- *master_id = hash(nhs_number + salt) - never changes, enables joins*
- *encrypted_pseudonym = encrypt(dataset_key, nhs_number) - rotatable per dataset/timeframe*

Evaluation:

| Criteria                    | Assessment | Notes                                                    |
|-----------------------------|------------|----------------------------------------------------------|
| Security & Compliance       | Good       | Key compromise limited to single dataset/timeframe       |
| Determinism & Joinability   | Perfect    | Joins always work via master_id                          |
| Performance & Scalability   | Excellent  | No re-encryption required                                |
| Operational Maintainability | Moderate   | Dual-layer architecture complexity                       |
| Resilience & Availability   | Good       | No downtime during rotation                              |
| Auditability & Governance   | Good       | Track both master_id and key versions                    |
| Future-Proofing             | Excellent  | Hash remains constant while encryption algorithms change |

## Key Rotation & Emergency Procedures

### Option 1: Single Long-Lived Master Key

Planned Rotation:
- Not possible - any key change breaks all existing pseudonyms
- Impact: Complete system rebuild required

Emergency (Key Compromise):
- Process: Generate new key -> Re-encrypt ALL data -> Deploy everywhere simultaneously
- Impact: Total system downtime, all systems must coordinate

### Option 2: Key Versioning with Batch Re-encryption

Planned Rotation:
- Process: Generate new key -> Re-encrypt all datasets -> Deploy new version
- Impact: Joins broken until all systems updated to same version

Emergency (Key Compromise):
- Same process as planned rotation but with immediate execution
- Key advantage over Option 1: Only data encrypted with the compromised key version needs re-encryption, not all historical data
- Impact: Pseudonyms from compromised time period must be recreated, coordination across all providers required
- Scope limitation example: If quarterly rotation is used, only 3 months of data requires re-encryption vs. entire historical dataset

### Option 3: Hash-based Master ID + Encrypted Pseudonym

Planned Storage Key Rotation:
- Process: Generate new key for single dataset/ timeframe -> New records use new key
- Impact: Zero downtime, joins unaffected, independent per dataset

Emergency Storage Key (Single Dataset):
- Process: 
  1. Revoke compromised key immediately
  2. Generate emergency replacement key  
  3. Re-encrypt affected historical data (cannot leave compromised data accessible)
  4. Destroy old key after re-encryption complete
- Impact: Single dataset affected, joins preserved via master_id

Emergency Hash Salt Compromise (Critical but rare):
- Process: Generate new salt -> Regenerate all master_ids across system
- Impact: System-wide master_id regeneration required

## Comparison: What Needs Recreation During Rotation

| Scenario            | Option 1         | Option 2               | Option 3                    |
|---------------------|------------------|------------------------|-----------------------------|
| Planned Rotation    | Not possible     | All pseudonyms         | Nothing (storage keys only) |
| Emergency Key       | Entire system    | All pseudonyms         | Single dataset only         |
| Downtime Required   | Complete rebuild | During re-encryption   | None (storage keys)         |
| Join Impact         | Total system     | Until all systems sync | None                        |
| Coordination Needed | All systems      | All systems            | Per dataset                 |

## Recommendation: Hash-based Master ID + Encrypted Pseudonym (Option 3)

Rationale:

- Minimal impact rotations: Storage keys rotate independently without affecting joins
- Emergency isolation: Key compromise limited to single dataset
- Zero downtime: Joins continue working during all rotation scenarios  
- Future-proof: Algorithm migration preserves existing joins via stable hash
- Operational simplicity: No cross-system coordination required for routine rotation

Key Benefits:
- Joins never break during key rotation
- Security compromise affects minimal data scope
- Each dataset can rotate keys independently
- No system-wide downtime requirements
