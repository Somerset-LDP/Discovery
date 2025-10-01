# Data Pipeline Architecture Principles and Options

This document outlines the principles, design choices, and migration approach for our data pipeline. It is intended for data engineers contributing to or extending this project.

# Layered Data Architecture

Our architecture is designed around **progressive refinement of data** with each layer having a **single, clear purpose**.  
This avoids muddling responsibilities and ensures transformations are both explainable and auditable.  

---

## 1. Pseudonymised Layer
- **Purpose**: A safe, minimal representation of the raw input feeds.  
- **Characteristics**:
  - **Immediate pseudonymisation**: PII is stripped or replaced at ingestion.  
  - **No interpretation**: We do not resolve conflicts or apply business rules.  
  - **No canonicalisation**: Data remains in feed-specific structures.  
  - **Lightweight enrichment only when unavoidable**: e.g., computing age at ingestion when the input feed only provides DOB.  
  - **Two types of outputs**:
    - *Raw-like pseudonymised*: structurally close to the source.  
    - *Calculated pseudonymised*: derived values from raw PII (e.g., age), computed before PII is discarded.  

👉 Think of this layer as **“dumb but safe.”**  

---

## 2. Refined Layer
- **Purpose**: The source of truth for **clean, consistent, canonical data models**.  
- **Characteristics**:
  - **Conflict resolution**: e.g., when multiple feeds provide height, weight, or demographic info, this is where we decide the current “truth.”  
  - **Canonical models**: patient, encounter, observation, etc., structured to reflect business semantics rather than feed idiosyncrasies.  
  - **Consistency rules**: enforce data quality, type alignment, reference integrity.  
  - **Feed agnostic**: once in refined, the data is no longer tied to the quirks of a source system.  

👉 This is the **business-ready foundation** for analytics and downstream processing.  

---

## 3. Derived Layer
- **Purpose**: Data optimised for **consumption and insight**.  
- **Characteristics**:
  - **Transformations for analytics**: aggregations, KPIs, trends.  
  - **Denormalisation for performance**: e.g., star schemas, materialised views.  
  - **Dashboards, reporting, ML features**: all consume from here.  

👉 This is the **“answer layer”** — where data is shaped to meet specific analytical or product needs.  

---

## Guiding Principles
- **Each layer has one job**:
  - *Pseudonymised*: strip PII, make data safe.  
  - *Refined*: resolve, standardise, canonicalise.  
  - *Derived*: optimise for consumption.  
- **Never re-identify**: once pseudonymised, data is never re-linked to PII.  
- **Progressive enrichment**: only done at the right layer, avoiding premature interpretation.  
- **Auditability**: every field can be traced back to source feeds via the pseudonymised layer.  


## Pseudonymised Layer

The **Pseudonymised Layer** is the first stage of our data pipeline, designed to handle personally identifiable information (PII) safely while supporting initial enrichment and transformation. Data in this layer is **pseudonymised before it is persisted**, ensuring no raw PII is stored.

### Layer Purpose

- **Redact PII** from incoming raw feeds.
- Perform minimal enrichment or calculated values that require PII as input.
- Maintain a feed- and date-oriented structure for easy partitioning and incremental processing.
- Separate data that is near-raw from data that has already been derived or calculated from raw input.

### Storage Structure

Data is stored in an **object storage (e.g., S3, GCS, or Azure Blob)** using a feed-first hierarchy. Each feed has its own top-level directory, partitioned by date, with separate subdirectories for near-raw pseudonymised data and calculated/derived data:

```
### Pseudonymised Layer Directory Structure

pseudonymised/
├── feed_a/
│   └── YYYY/
│       └── MM/
│           └── DD/
│               ├── raw/          # Pseudonymised, near-raw data
│               └── calculated/   # Derived/enriched data from raw input
├── feed_b/
│   └── YYYY/
│       └── MM/
│           └── DD/
│               ├── raw/
│               └── calculated/

```


- **Feed**: Top-level directory representing the source of the data.
- **Date partitioning**: Organises data by ingestion or snapshot date, supporting incremental processing.
- **`raw/`**: Stores data closely resembling the original input, but with all PII pseudonymised.
- **`calculated/`**: Stores data derived from the raw input (e.g., initial enrichments, computed metrics).

### Notes

- This layer is **transient in nature**; it is not intended for canonical modeling or conflict resolution. That is handled in the **Refined Layer**.
- Pipelines operating on this layer are split into two sub-pipelines:
  - `pipeline_pseudonymised_raw.py` → outputs near-raw pseudonymised data
  - `pipeline_pseudonymised_enriched.py` → outputs calculated/enriched data derived from raw input
- Keeping raw-like and calculated data separate simplifies processing and ensures clarity in downstream transformations.


## Pipeline Layers

### 1. Pseudonymised Layer (entry point)
- **Script**: `pipeline_pseudonymised.py`
- **Role**: Orchestrates ingestion of incoming raw data.
- Immediately triggers sub-pipelines to handle pseudonymisation and enrichment.

#### Sub-pipelines
- **`pipeline_pseudonymised_raw.py`**  
  - Strips or replaces all PII fields.  
  - Outputs data that is *structurally close to the raw feed* but pseudonymised.  
  - This dataset acts as the safe baseline for downstream use.

- **`pipeline_pseudonymised_enriched.py`**  
  - Runs enrichments that **depend on PII** at calculation time (e.g., age at ingestion).  
  - Ensures PII is pseudonymised *before persistence*.  
  - Outputs new pseudonymised data types that complement the pseudonymised raw.

---

### 2. Refined Layer
- **Script**: `pipeline_refined.py`
- **Role**:  
  - Consumes outputs of the pseudonymised raw and pseudonymised new data.  
  - Standardises formats, applies harmonisation, and builds relational models.  
  - Designed for data engineers and analysts to work with directly.  

---

### 3. Derived Layer
- **Script**: `pipeline_derived.py`
- **Role**:  
  - Consumes from the refined layer.  
  - Produces analytics-ready datasets and aggregates.  
  - Optimised for reporting, dashboards, and machine learning features.

---

## Principles

- **Separation of Concerns**: Each pipeline handles only its own transformations.  
- **Immediate Pseudonymisation**: PII never leaves the raw entry point unprocessed.  
- **Modular Orchestration**: `pipeline_raw.py` coordinates sub-pipelines, keeping logic isolated.  
- **Future-Proofing**: Any stage can be reimplemented (e.g., scaling out with Spark) without affecting the others.  
- **Auditability**: Each layer is a checkpoint, supporting lineage and troubleshooting.  


## 1. Separation of Data Layers

We organize data into three distinct layers:

| Layer   | Purpose | Storage |
|---------|---------|---------|
| **Raw** | Store incoming data in its original form; immutable | Parquet / Object store |
| **Refined** | Transform and standardize data for analysis; canonical model | Relational DB (Postgres) |
| **Derived** | Compute metrics, aggregates, and analytics-ready datasets | Relational DB (Postgres) |

**Principles:**
- Keep layers separate to control transformations and maintain lineage.
- Compute-intensive transformations are isolated per layer, allowing independent scaling.
- Data ownership is explicit: only one process should update a given dataset.

**Current Choice:**  
- Raw: Parquet in a data lake  
- Refined & Derived: Postgres schemas (separate) in the same database  

**Flexibility for Future:**  
- We can move layers to separate databases, distributed systems, or cloud-native warehouses as required.

---

## 2. Triggering Pipeline Jobs

We considered two main options:

1. **Scheduled Jobs (time-based)**  
   - Simple, predictable, easy to reason about.  
   - Good for batch-oriented pipelines where data arrives periodically.  

2. **Event-Driven Jobs (data-change triggered)**  
   - Reactive, low-latency.  
   - Risk of cascade cycles if multiple jobs update the same derived data.  

**Current Choice:**  
- Scheduled jobs orchestrated via a DAG-based system (Airflow, Prefect, or Dagster) for predictable execution.  

**Future Flexibility:**  
- Event-driven triggers can be added later for low-latency scenarios, managed under the same orchestration to prevent cycles.

---

## 3. Managing Updates to Derived Data

**Challenge:** Prevent multiple pipelines from updating the same derived datasets and avoid unnecessary recomputation.

**Principles:**
- Each derived table has a single “owner” pipeline.  
- Idempotent pipeline design ensures re-runs do not corrupt data.  
- Metadata tracking defines which job produces each derived table.  

**Implementation Strategy:**  
- Recompute metrics incrementally: only recalc BMI or weight category when height/weight changes.  
- Record “latest snapshot” in derived tables, avoiding full recomputation while supporting historical analysis later.

**Future Flexibility:**  
- A versioned model can be layered in to track historical records and maintain lineage without redesigning the pipeline.

---

## 4. Tracking History and Lineage

**Principles:**
- Refined layer contains canonical, standardized data.  
- Derived layer can contain snapshots or incremental calculations.  
- Audit trails and lineage metadata ensure analysts can understand the origin and transformation of any value.

**Migration Approach:**
1. Start with **latest snapshot** model for simplicity.  
2. Introduce change-tracking or versioned tables if historical analysis requirements increase.  
3. Maintain separation of layers to allow independent scaling and evolution.

---

## 5. Summary of Design Philosophy

- **Separation of concerns:** raw, refined, derived layers.  
- **Explicit ownership:** only one process writes each derived dataset.  
- **Incremental computation:** reduce recomputation and improve efficiency.  
- **Orchestration-managed triggers:** prevent cascade cycles and control dependencies.  
- **Auditability and lineage:** prepare for future data governance requirements.  
- **Scalability & flexibility:** architecture allows swapping storage technologies or adopting event-driven pipelines in the future without major redesign.


TODO

Move liniege out of observation - experiement with one of the options below

2. Data Lake / File-Based Lineage

Raw files stored in a data lake / object store.

Canonical table stores unique IDs that can be mapped back to files (filename + row ID optional).

Optionally, use metadata/catalog tools to track lineage (see below).

3. Provenance/Lineage Tools

Dedicated tools/systems: e.g., OpenLineage, DataHub, Apache Atlas.

Track lineage automatically from ingestion pipelines:

Source table/file → transformation → target table/view.

Pros:

Decouples lineage from schema.

Supports visual lineage and impact analysis.

Handles multiple data sources and transformations elegantly.

4. Hybrid

Keep minimal lineage keys in the refined table (e.g., raw_record_id),

Store detailed provenance in a lineage table or catalog.

## Connect to FHIR terminology server to validate code systems and to perform mappings
## Connect to a filter which acts as a whitelist, only allowing records to pass through that meet the filter criteria
## Incorporate lineage. I'd like to be able to explore different options
## Incorporate Data dictionary to allow people to understand the data model. I'd like to be able to explore different options
## I'd like to explore different storage options including Lakehouse. I am not sure yet of the best way to store the data (raw, refined, reporting)
## I'd also like to explore scaling, redundancy and failover
## Add a Self serve interface with something like Trino so that people with SQL skills can explore the data (this is related to the Data dictionary feature)
## Add an Audit trail for who is doing what and when
## Add monitoring and alerting
## Add a Pseudonymisation step
## Merging records
## PII detection before writting to pseudonymised layer

## Timeout, network error, retry logic

## Tests
If you like, I can also add a test for aggregating patients into age ranges directly from mock records, so you can verify your age-bracket logic without querying the DB.

Extend this test file to include an end-to-end test that simulates reading multiple mock records, validating, transforming, and checking calculated BMI, without touching the database. This gives a lightweight pipeline test.
