# Age-BMI Data Pipeline - Technical Proof of Concept

This project demonstrates and de-risks key architectural decisions for the Somerset LDP's data platform. Specifically, it validates:

**Data Flow Architecture**: How to structure data pipelines with clear separation of concerns - each processing stage has a single responsibility and well-defined boundaries. The pipeline demonstrates why layered data architecture enables independent scaling, testing, and technology choices while ensuring PII protection from the outset.

**Clinical Classification Integration**: How BMI categorisation uses evidence-based clinical guidelines rather than arbitrary thresholds - implementing WHO adult BMI categories with ethnicity-specific adjustments and UK90 pediatric centiles through FHIR ObservationDefinitions and the rcpchgrowth library.

**Healthcare Standards Integration**: How FHIR terminology servers provide authoritative clinical code validation, reference ranges, and unit conversions, ensuring analytical outputs meet clinical interoperability standards without custom terminology management.

## Architecture Overview

Our architecture is built on **progressive refinement of data** with each layer having a **single, clear purpose**. This separation of concerns ensures transformations are explainable, auditable, and maintainable.

```mermaid
flowchart LR
    A["🔴 Raw Data<br/>• PII mixed<br/>• Untrusted<br/>• Variable quality<br/>• Unsafe"] 
    B["🟡 Pseudonymised<br/>• PII removed<br/>• Feed-native<br/>• Minimal processing<br/>• Data added<br/>• Safe but unrefined"]
    C["🟢 Refined<br/>• Canonical<br/>• Validated<br/>• Conflict resolved<br/>• Business entities"]
    D["🔵 Derived<br/>• Optimized<br/>• Aggregated<br/>• Performant<br/>• Consumer ready"]
    
    A -->|"Never persisted"| B
    B -->|"Immediate PII removal"| C
    C -->|"Source of truth"| D
    D -->|"Applications consume"|E[Analytics<br/>Reports<br/>ML Models<br/>Dashboards]
    
    style A fill:#ffebee
    style B fill:#fff8e1
    style C fill:#e8f5e8
    style D fill:#e3f2fd
    style E fill:#f8f9fa,stroke:#6c757d,stroke-dasharray: 5 5
```

**Key Principles:**
- **Left-to-right flow**: Data becomes progressively safer, cleaner, and more usable
- **Single responsibility**: Each layer has one clear purpose and transformation type
- **Immutable progression**: Upstream layers are never modified by downstream processes
- **Quality gates**: Each transition includes validation and quality checks

---

## Data Architecture Layers

### 1. Pseudonymised Layer
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

**Think of this layer as "dumb but safe"** - minimal processing, maximum safety.

### 2. Refined Layer  
**Purpose**: The source of truth for clean, consistent, canonical data models.

**Characteristics**:
- **Conflict resolution**: When multiple feeds provide conflicting data, business rules determine truth
- **Canonical models**: Standard patient, encounter, observation entities reflecting business semantics
- **Data quality enforcement**: Type validation, reference integrity, consistency rules
- **Feed-agnostic**: Data structure no longer tied to source system quirks
- **FHIR integration**: Code system validation and terminology mapping

**Storage**: Relational database with normalized schema - see [`create_schema_refined.sql`](data/init/ddl/create_schema_refined.sql) for complete schema definition.

**This is the business-ready foundation** for all downstream analytics and processing.

### 3. Derived Layer
**Purpose**: Data optimized for consumption, reporting, and machine learning.

**Characteristics**:
- **Analytics transformations**: BMI calculations, risk scores, trend analysis
- **Performance optimization**: Denormalized views, pre-computed aggregates
- **Incremental updates**: Only recalculate when upstream data changes

**Storage**: Multi-modal storage optimized for different analytical needs:
- *Relational tables* for traditional BI and reporting (see [`create_schema_derived.sql`](data/init/ddl/create_schema_derived.sql))
- *Columnar formats* (Parquet, Delta) for ML feature stores and large-scale analytics
- *Time-series databases* for trend analysis and monitoring dashboards
- *Document stores* for flexible analytical metadata and complex nested structures

**This is the "answer layer"** where data is shaped for specific analytical needs.

---

## Pipeline Implementation

The pipeline implementation follows the **single responsibility principle** - each pipeline script is dedicated to populating one specific storage layer. This modular approach ensures that:

- **Clear boundaries**: Each pipeline has a well-defined input source and output destination
- **Independent execution**: Pipelines can be run separately for testing, debugging, or selective processing
- **Failure isolation**: Issues in one layer don't cascade to others - failed pipelines can be re-run independently  
- **Scalable orchestration**: Each pipeline can be scheduled, monitored, and scaled based on its specific requirements
- **Technology flexibility**: Different layers can use different processing technologies without affecting others

The current implementation provides three discrete pipelines, each responsible for a single architectural layer:

### 1. Pseudonymised Layer (`pipeline_pseudonymised.py`)
- **Role**: Entry point orchestrating raw data ingestion
- **Sub-pipelines**:
  - `pipeline_pseudonymised_raw.py`: PII removal while preserving structure
  - `pipeline_pseudonymised_enriched.py`: PII-dependent calculations (age from DOB)

### 2. Refined Layer (`pipeline_refined.py`)  
- **Role**: Canonicalization and standardization
- **Features**:
  - FHIR terminology server integration for code validation
  - Data quality validation and conflict resolution
  - Canonical model construction

### 3. Derived Layer (`pipeline_derived.py`)
- **Role**: Analytics preparation  
- **Features**:
  - BMI calculation and weight classification
  - Age-appropriate BMI thresholds (adult vs pediatric)
  - Ethnicity-specific BMI categories
  - Population-level aggregations

---

## Clinical Standards Integration

Healthcare data pipelines must ensure clinical accuracy and interoperability by integrating with established healthcare standards and terminologies. **Clinical Standards Integration** means embedding validated healthcare coding systems, reference ranges, and classification logic directly into the data processing workflows - not as an afterthought, but as a core architectural component.

This integration serves two critical functions: **terminology services** provide the infrastructure for code validation and mapping between healthcare code systems, while **clinical classification logic** ensures that analytical outputs (like BMI categories) follow evidence-based clinical guidelines rather than arbitrary technical thresholds.

### FHIR Server
The FHIR server functions as a **specialized reference data layer for healthcare** - providing authoritative definitions, mappings, and validation rules for clinical codes and concepts. Rather than inventing custom reference data management, we chose FHIR because it's the established international standard with existing tooling, comprehensive code system support, well modelled healthcare concepts and built-in interoperability with other healthcare systems.

The pipeline integrates with a HAPI FHIR server to ensure clinical data accuracy:

- **Code System Validation**: Verify SNOMED CT, LOINC, and other healthcare codes
- **Terminology Mapping**: Translate between code systems (e.g., LOINC → SNOMED)
- **Reference range definition**: Determine if a value is within a given reference range e.g. overweight BMI category
- **Observation unit of measure definition**: Define the units that a given Observation should be in. This is used to both validate observation data and to facilitate unit conversion to the LDP's canonical units in the refined layer

### BMI Classification Logic
While the FHIR server provides the infrastructure for code validation and mapping, **BMI Classification Logic** implements the clinical decision-making rules that transform raw measurements into meaningful health assessments. This logic embeds clinical guidelines directly into the data pipeline, ensuring that BMI categories reflect current medical practice rather than arbitrary numerical ranges.

The classification system leverages FHIR-defined ValueSets and ConceptMaps to maintain consistency with international clinical standards, while accommodating population-specific variations in health risk thresholds.

Implements clinically appropriate BMI categories based on:

- **Age Groups**: Adult (≥18 years) vs pediatric classifications
- **Ethnicity**: Different BMI thresholds for different populations  
- **Clinical vs Surveillance**: Different centile thresholds for screening vs clinical intervention
- **Growth Charts**: UK90 charts for pediatric BMI centiles using [`rcpchgrowth`](https://growth.rcpch.ac.uk/developer/rcpchgrowth/) library

**Classification Resources**:
- [`adult-who-bmi-clinical-classification`](data/fhir-store/resources/observationdefinition/adult_who_bmi_clinical_classification.json) - Adult BMI categories by ethnicity
- [`child-uk90-bmi-clinical-classification`](data/fhir-store/resources/observationdefinition/child_uk90_bmi_clinical_classification.json) - Pediatric BMI centiles (clinical thresholds)  
- [`child-uk90-bmi-surveillance-classification`](data/fhir-store/resources/observationdefinition/child_uk90_bmi_surveillance_classification.json) - Pediatric BMI centiles (surveillance thresholds)

---

## The Demo

### Quick Start

1. **Start Docker services:**
See [docker-compose.yml](./docker-compose.yml) for details of the services

   ```bash
   docker-compose up -d
   ```

2. **Environment variables are configured in `.env` file:**
   - The `.env` file in this directory contains all required environment variables
   - Variables are automatically loaded when running the demo script

| Environment Variable | Description | Example Value (based on docker compose file) |
|---------------------|-------------|---------------|
| `SNOMED_BODY_HEIGHT` | SNOMED CT code for body height observations (50373000 = "Body height") | `50373000` |
| `SNOMED_BODY_WEIGHT` | SNOMED CT code for body weight observations (27113001 = "Body weight") | `27113001` |
| `REFINED_DATABASE_URL` | PostgreSQL connection string for the refined data layer database | `postgresql+psycopg2://admin:admin@localhost:5432/ldp` |
| `DERIVED_DATABASE_URL` | PostgreSQL connection string for the derived data layer database | `postgresql+psycopg2://admin:admin@localhost:5432/ldp` |
| `FHIR_BASE_URL` | Base URL for the HAPI FHIR server used for terminology services | `http://localhost:8080/fhir` |

3. **Run the demo:**
   ```bash
   # From the age-bmi directory (where your virtual environment is active)
   python demo.py demo/raw_patients.json
   ```

### Sample Data

- `raw_patients.json` - Sample patient data demonstrating various:
  - Age groups (adults and children)
  - Genders (Male/Female)
  - Ethnicities (White British, British Asian)
  - Units (kg/lbs/stone for weight, cm/m/inches for height)
  - Code systems (SNOMED CT, LOINC)

| ID | Age | Gender | Ethnicity | Height | Weight |
|----|-----|--------|-----------|--------|--------|
| 1 | 40 | Male (248152002) | White British (494131000000105) | 65 inches | 154 lbs |
| 2 | 47 | Female (248153007) | White Irish (92681000000104) | 172 cm | 75 kg |
| 3 | 10 | Female (248153007) | White British (494131000000105) | 140 cm | 35 kg |
| 4 | 8 | Male (248152002) | White Irish (92681000000104) | 47.2 inches | 58.8 lbs |

---

## Technology Stack

### Core Components
- **Language**: [Python 3.12+](https://www.python.org/)
- **Database**: [PostgreSQL](https://www.postgresql.org/) (refined/derived layers)  
- **Object Storage**: File system (future: [AWS S3](https://aws.amazon.com/s3/)/[Google Cloud Storage](https://cloud.google.com/storage)/[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs))
- **FHIR Server**: [HAPI FHIR](https://hapifhir.io/) 
- **Testing**: [pytest](https://pytest.org/) with [Docker testcontainers](https://testcontainers.com/)

### Key Libraries
- **Healthcare**: [`fhirclient`](https://github.com/smart-on-fhir/client-py), [`rcpchgrowth`](https://github.com/rcpch/rcpchgrowth) (UK growth charts)
- **Data Processing**: [`pandas`](https://pandas.pydata.org/), [`sqlalchemy`](https://www.sqlalchemy.org/)  
- **Testing Infrastructure**: [`testcontainers`](https://github.com/testcontainers/testcontainers-python) for integration tests

### Development Infrastructure
- **Containerization**: [Docker Compose](https://docs.docker.com/compose/) for local FHIR server and Postgres server

---

## Current Implementation Status

### Implemented Features
- **Three-layer pipeline architecture** with clear separation of concerns
- **BMI calculation and clinical classification** for adults and children  
- **FHIR terminology integration** with code validation and mapping
- **Ethnicity-specific BMI thresholds** following clinical guidelines
- **SNOMED CT sex code mapping** for demographic standardization

### Proof of Concept Limitations
This is a **technical demonstration** with several production readiness gaps:

- **No production FHIR server**: Uses local HAPI FHIR instance
- **File-based object storage**: Not using cloud storage services
- **Single database instance**: No separation of refined/derived databases  
- **No orchestration**: Manual pipeline execution, no scheduling
- **Limited security**: No authentication, authorization, or audit logging
- **No monitoring**: No observability, alerting, or performance metrics
- **No data lineage**: Missing ability to trace data through the storage layers
- **No audit trail**: Missing audit of who, what and when
- **No merging of data**: Updated records, data conflicts and de-duplication are not handled
- **No pseduonymisation**: Pseudonymisaton is the subject of its own technical test

---

## Future Development Priorities

### 1. Production Readiness
- **Security**: Authentication, authorization, audit trails
- **Orchestration**: for scheduled execution
- **Monitoring**: Observability, alerting, performance tracking
- **Scaling**: Separate databases, distributed processing capabilities
- **Cloud Integration**: S3/GCS storage, managed services

### 2. Data Architecture Evolution  
- **Lineage Tracking**: Investigate OpenLineage, DataHub, or Apache Atlas
- **Data Dictionary**: Self-service data discovery and documentation
- **Lakehouse Architecture**: Explore Delta Lake, Iceberg for unified storage
- **Stream Processing**: Event-driven pipelines for real-time updates (this is relevant when the LDP needs to support SIDeR)

### 3. Healthcare-Specific Features
- **PII Detection**: Automated identification of personal information
- **Record Linkage**: Probabilistic matching across data sources  

### 4. Analytics & Self-Service
- **Query Interface**: Trino/Presto for SQL-based data exploration
- **Data Catalog**: Automated metadata management and discovery

---

## Project Structure
```
age-bmi/
├── pipeline_refined.py       # Refined layer pipeline
├── pipeline_derived.py       # Derived layer pipeline  
├── pipeline.py               # Legacy pipeline (being replaced)
├── docker-compose.yml        # FHIR server and Postgres server
├── calculators/              # BMI calculation logic
├── fhir/                     # FHIR server integration
├── data/                     # Data definitions and reference data
│   ├── fhir-store/           # FHIR resources and terminology
│   ├── init/                 # Database initialization scripts
│   └── pseudonymised-store/  # Raw data schema
├── tests/                    # Unit and integration tests
│   ├── fixtures/             # Shared test infrastructure
│   ├── unit/                 # Isolated component tests  
│   └── integration/          # End-to-end pipeline tests
└── docker-compose.yml        # Local FHIR server setup
```

---

## Contributing

This proof of concept demonstrates key architectural principles for the LDP's data pipelines. The modular design allows individual components to be enhanced or replaced as requirements evolve.

**Key Design Principles**:
- **Single Responsibility**: Each layer and pipeline has one clear purpose
- **Immutable Layers**: Upstream data is never modified by downstream processes  
- **Clinical Accuracy**: Healthcare standards and terminologies are properly applied

### BMI Classification Naming Conventions

When contributing new BMI ObservationDefinition resources, please follow the established naming convention to maintain consistency and discoverability:

**Format**: `{population}_{standard}_{metric}_{purpose}_classification.json`

**Components**:
- **Population**: `adult` | `child` | `pediatric` | `all_ages`
- **Standard**: `who` | `uk90` | `cdc` | `iotf` | `custom`  
- **Metric**: `bmi` | `weight` | `height` | etc.
- **Purpose**: `clinical` | `surveillance` | `research` | `screening`

**Examples**:
- `adult_who_bmi_clinical_classification.json` - WHO adult BMI thresholds for clinical use
- `child_uk90_bmi_surveillance_classification.json` - UK90 pediatric BMI centiles for population surveillance
- `adult_custom_bmi_research_classification.json` - Custom BMI categories for research studies

For questions about implementation details or architecture decisions, refer to the comprehensive test suite and inline documentation.