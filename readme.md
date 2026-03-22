# Autonomous Hybrid Ingestion Framework (Assignment 2)

Group Name - Schemeless
Members - Shardul Junagade, Kaushal Bule, Akash Gupta

This repository contains the Assignment 2 submission for a metadata-driven hybrid data platform that ingests JSON, decides storage placement automatically, and exposes metadata-driven CRUD operations.

## Assignment Objective

The system extends Assignment 1 by implementing an intelligence layer that can:

- register and version user schemas,
- classify incoming fields into SQL, MongoDB, Buffer, or Both,
- normalize repeating relational entities automatically,
- decide embed vs reference strategy for MongoDB,
- persist decision metadata for explainability,
- generate CRUD plans dynamically from metadata and return merged JSON responses.

## Submission Highlights

- schema registry with active version and history,
- field-level placement decisions with reasons,
- Buffer lifecycle with persistence and drain/reconciliation,
- SQL normalization for arrays of objects,
- SQL normalization for primitive arrays,
- SQL normalization for repeated scalar groups (for example phone1, phone2, phone3),
- score-based Mongo embed/reference strategy,
- persisted Mongo decision telemetry (score, threshold, reasons, hints),
- metadata-driven CRUD engine,
- test suite covering core Assignment 2 behavior.

## Repository Components

- main.py
- data_consumer.py
- ingestion_pipeline.py
- query_engine.py
- metadata_store.py
- database_managers.py
- placement_heuristics.py
- view_databases.py
- tests/test_assignment2_pipeline.py
- docs/ASSIGNMENT2_TECHNICAL_REPORT.md
- docs/ARCHITECTURE_ASSIGNMENT2.md
- docs/CRUD_JSON_INTERFACE.md
- reports/Schemeless_A2.tex

## End-to-End Architecture

1. Schema Registration - User schema is registered and versioned in metadata.

2. Metadata Interpretation - Recursive path tracking stores frequency, type distribution, and sample values.

3. Classification - Fields are routed to SQL, MongoDB, Buffer, or Both using heuristics and structure checks.

4. SQL Strategy
- Root scalar fields are stored in ingested_records.
- Arrays of objects become child tables (norm_<entity_path>).
- Primitive arrays become value child tables.
- Repeated scalar groups become normalized child tables.
- Child tables use parent_sys_ingested_at foreign keys with ON DELETE CASCADE.

5. Mongo Strategy
- Nested entities are evaluated with a score-based rule.
- Mode is selected as embed or reference.
- Decision telemetry is persisted for debugging/reporting.
- Reference mode writes to dedicated collections such as ref_orders.

6. Buffer Pipeline
- Low-evidence fields are staged in Buffer.
- Buffered records are persisted in buffer_records.
- When a field resolves, buffered values are drained to the final backend.
- Startup reconciliation attempts to drain historical resolved buffer residues.

7. Query Generation
- CRUD requests are interpreted using metadata mappings.
- SQL and Mongo queries are generated dynamically.
- Results are merged into one JSON response.

## SQL Normalization Rules

- Arrays of objects: normalize into child tables.
- Primitive arrays: normalize into value tables with value and item_index.
- Repeated scalar groups: collapse into one normalized child entity.
- Root table key: sys_ingested_at.
- Child foreign key: parent_sys_ingested_at -> ingested_records(sys_ingested_at).

## MongoDB Strategy Rule

Reference score is computed from:

- array size,
- array-of-objects presence,
- object width,
- nesting depth,
- likely shared-entity signal,
- schema hints (frequently_updated, shared, unbounded, expected_max_items).

Decision:

- score >= threshold (current threshold: 2) -> reference,
- score < threshold -> embed.

Nested structural fields (dict/list) bypass warm-up buffering and route directly to MongoDB.

## Metadata Model

Core metadata keys include:

- schema_registry,
- fields,
- placement_decisions,
- field_mappings,
- buffer,
- normalization,
- mongo_strategy,
- total_records and timeline keys.

Mongo strategy telemetry stored per entity includes:

- mode,
- collection,
- decision_score,
- reference_threshold,
- decision_reasons,
- schema_hints.

## CRUD Interface

Operation types:

- insert,
- read,
- update,
- delete.

Update strategy currently uses delete_then_insert to preserve cross-backend consistency.

Example usage:

```python
from ingestion_pipeline import IngestionPipeline

pipeline = IngestionPipeline()

pipeline.execute_crud(
    {
        "operation": "insert",
        "data": {
            "username": "alice",
            "email": "alice@example.com",
            "orders": [
                {"order_id": "o1", "item": "book", "quantity": 1, "price": 12.5}
            ],
        },
    }
)

read_result = pipeline.execute_crud(
    {
        "operation": "read",
        "fields": ["username", "email", "orders"],
        "filters": {"username": "alice"},
    }
)

print(read_result)
pipeline.close()
```

## Quick Run Guide

This section is intentionally kept aligned with README_SHORT.md.

### 1) Create virtual environment and install dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
pip install -r requirements.txt
```

### 2) Start generator API (terminal 1)

```bash
uvicorn main:app --reload --port 8000
```

### 3) Run ingestion (terminal 2)

```bash
python data_consumer.py
```

Quickstart with detailed logs:

```bash
python quickstart.py --verbose
```

Optional:

```bash
python data_consumer.py 50 20
python data_consumer.py 50 20 path/to/schema.json
```

### 4) Run tests

```bash
pytest -q
```

### 5) Clean generated files

```bash
python quickstart.py clean --yes
```

This cleans local artifacts and also drops project MongoDB databases:

- ingestion_db
- assignment2_test_db

Keep MongoDB data (files/cache only):

```bash
python quickstart.py clean --no-mongo
```

### 6) View docs

- docs/ASSIGNMENT2_TECHNICAL_REPORT.md
- docs/ARCHITECTURE_ASSIGNMENT2.md
- docs/CRUD_JSON_INTERFACE.md

## Database Inspection and Debugging

The repository includes a viewer tool:

```bash
python view_databases.py
```

Useful commands:

```bash
python view_databases.py placements
python view_databases.py normalization
python view_databases.py mongo_strategy
python view_databases.py buffer
python view_databases.py search orders
```

## Testing Status

Run:

```bash
pytest -q
```

Current suite includes Assignment 2 checks for:

- schema registration,
- buffer transition and drain behavior,
- SQL normalization behavior,
- metadata-driven CRUD cycle,
- Mongo decision telemetry persistence,
- nested-field routing behavior,
- generator endpoint and structure checks.

## Ingestion Benchmark (Single Batch)

To provide measurable runtime evidence, ingestion was benchmarked for one batch each at sizes:

- 10, 20, 50, 100, 500, 1000

Method used:

- one run per batch size,
- fresh isolated SQL file, metadata file, and Mongo database name for each run,
- same API endpoint and consumer flow as normal ingestion.

Measured results:

| Batch Size | Elapsed (s) | Per Record (ms) | Throughput (rec/s) |
|---:|---:|---:|---:|
| 10 | 3.3421 | 334.211 | 2.99 |
| 20 | 16.0346 | 801.729 | 1.25 |
| 50 | 23.4952 | 469.903 | 2.13 |
| 100 | 47.9602 | 479.602 | 2.09 |
| 500 | 277.1893 | 554.379 | 1.80 |
| 1000 | 538.6599 | 538.660 | 1.86 |

Raw benchmark artifact is stored at:

- docs/ingestion_benchmark_results.json


## Notes and Constraints

- sys_ingested_at is the cross-backend join key.
- Buffer data may include historical records from earlier runs unless cleaned.
- Update currently favors consistency over minimal write cost.
