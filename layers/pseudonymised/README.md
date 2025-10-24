# Pseudonymised layer

**Purpose**: A safe, minimal representation of raw input feeds with immediate PII protection.

**Characteristics**:
- **Immediate pseudonymisation**: PII is stripped or hashed at ingestion - never persisted in raw form
- **No interpretation**: Data conflicts and business rules are not resolved at this stage  
- **Feed-specific structure**: Data retains original feed formats for auditability
- **Minimal enrichment**: Only computations requiring PII input (e.g., age calculation from DOB)
- **Two output types**:
  - *Raw-like pseudonymised*: Structurally close to source but PII-safe
  - *Calculated pseudonymised*: Derived values computed before PII disposal

**Storage**: Object store (S3/GCS/Azure) with feed-first hierarchy:
```
pseudonymised/
├── feed_a/YYYY/MM/DD/
│   ├── raw/          # Near-original structure, PII removed
│   └── calculated/   # Age, derived demographics
└── feed_b/YYYY/MM/DD/
    ├── raw/          # Near-original structure, PII removed
    └── calculated/   # Age, derived demographics
```