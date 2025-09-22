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

## Connect to terminology server
## Connect Barbara's cohort filter

## Tests
If you like, I can also add a test for aggregating patients into age ranges directly from mock records, so you can verify your age-bracket logic without querying the DB.

Extend this test file to include an end-to-end test that simulates reading multiple mock records, validating, transforming, and checking calculated BMI, without touching the database. This gives a lightweight pipeline test.
