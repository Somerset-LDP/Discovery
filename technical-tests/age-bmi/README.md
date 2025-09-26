# Data Pipeline Architecture Principles and Options

This document outlines the principles, design choices, and migration approach for our data pipeline. It is intended for data engineers contributing to or extending this project.

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

## Timeout, network error, retry logic

## Tests
If you like, I can also add a test for aggregating patients into age ranges directly from mock records, so you can verify your age-bracket logic without querying the DB.

Extend this test file to include an end-to-end test that simulates reading multiple mock records, validating, transforming, and checking calculated BMI, without touching the database. This gives a lightweight pipeline test.
