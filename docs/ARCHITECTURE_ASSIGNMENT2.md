# Assignment 2 Architecture Notes

## End-to-End Flow
1. Schema Registration
- Consumer optionally fetches and registers schema from the generator endpoint.
- Schema is versioned in metadata.

2. Ingestion
- Incoming JSON record is accepted by IngestionPipeline.
- Bi-temporal fields are enforced: t_stamp and sys_ingested_at.

3. Metadata Interpretation
- Recursive path tracking captures type and frequency statistics for every field path.

4. Classification
- Placement engine returns one of:
  - SQL
  - MongoDB
  - Buffer
  - Both

5. SQL Engine
- Root scalar fields routed to ingested_records.
- Repeating entities (arrays of objects) normalized into child tables.
- PK/FK and indexes are created automatically.

6. MongoDB Engine
- Nested data passes through embed/reference strategy.
- Embed data is stored in ingested_records documents.
- Referenced payloads go into path-specific collections.

7. Buffer Pipeline
- Undecided fields are stored as buffer observations.
- Buffered values are also written into buffer_records for temporary persistence.
- After enough observations, fields move from Buffer to final backend.

8. Query Generation Engine
- JSON CRUD request is interpreted using metadata mappings.
- SQL and Mongo queries are generated and executed.
- Results are merged to one JSON response.

## Key Internal Modules
- metadata_store.py:
  - Metadata persistence and schema registry
- placement_heuristics.py:
  - Routing policy including Buffer handling
- database_managers.py:
  - SQL normalized tables and Mongo multi-collection access
- ingestion_pipeline.py:
  - Assignment-2 orchestration
- query_engine.py:
  - Metadata-driven CRUD translator and executor

## Data Integrity
- sys_ingested_at is the cross-backend link key.
- SQL foreign keys enforce referential consistency.
- Update strategy uses delete_then_insert for schema-safe cross-backend mutation.
