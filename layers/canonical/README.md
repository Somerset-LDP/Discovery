# Canonical layer

**Purpose**: The source of truth for clean, consistent, canonical data models.

**Characteristics**:
- **Conflict resolution**: When multiple feeds provide conflicting data, business rules determine truth
- **Canonical models**: Standard patient, encounter, observation entities reflecting business semantics
- **Data quality enforcement**: Type validation, reference integrity, consistency rules
- **Feed-agnostic**: Data structure no longer tied to source system quirks
- **FHIR integration**: Code system validation and terminology mapping

**Storage**: Relational database with normalized schema

**This is the business-ready foundation** for all downstream analytics and processing.