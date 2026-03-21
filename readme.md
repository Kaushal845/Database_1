# Autonomous Hybrid Ingestion Framework (Assignment 2)

This project implements a metadata-driven hybrid database framework that automatically:
- Registers JSON schemas
- Classifies fields into SQL, MongoDB, Buffer, or Both
- Normalizes repeating entities into relational child tables
- Applies MongoDB embed/reference document strategies
- Generates and executes CRUD operations from JSON requests

The system extends Assignment-1 and now includes the full Assignment-2 architecture requirements.

## What Is New in Assignment 2

- Schema registration with version tracking
- Explicit Buffer pipeline for undecided fields
- SQL normalization engine for arrays of objects
- Mongo strategy engine for embed vs reference placement
- Metadata-driven CRUD query engine with merged JSON responses
- Pytest suite for Assignment-2 functionality

## Core Components

- main.py
  - FastAPI synthetic generator with nested/repeating entities
  - Endpoints: /, /record/{count}, /schema, /health
- data_consumer.py
  - Fetches stream data and feeds Assignment-2 pipeline
  - Optional schema registration support
- ingestion_pipeline.py
  - End-to-end orchestrator for schema, classification, normalization, routing, and CRUD execution
- query_engine.py
  - Metadata-driven CRUD planner and executor
- metadata_store.py
  - Persistent intelligence layer (schema versions, mappings, strategies, buffer state)
- database_managers.py
  - SQL manager (root + normalized child tables)
  - Mongo manager (multi-collection + in-memory fallback)
- placement_heuristics.py
  - Decision engine: SQL / MongoDB / Buffer / Both

## Architecture Summary

1. Schema Registration
- User schema is versioned in metadata

2. Metadata Interpretation
- Recursive field-path tracking captures frequency/type stability

3. Classification Engine
- Fields routed to SQL, MongoDB, Buffer, or Both

4. SQL Engine
- Root scalars go to ingested_records
- Arrays of objects become normalized child tables (norm_<entity>)
- FK: parent_sys_ingested_at -> ingested_records(sys_ingested_at)

5. Mongo Engine
- Nested fields are embedded or referenced based on size/shape heuristics
- Referenced entities are stored in dedicated collections (for example ref_orders)

6. Query Engine
- JSON CRUD requests are translated using metadata mappings
- SQL and Mongo queries are combined into a single JSON response

## Installation

Prerequisites:
- Python 3.8+
- MongoDB optional (system runs with in-memory Mongo fallback if unavailable)

Install dependencies:

```bash
pip install -r requirements.txt
```

## Running the System

### 1. Start generator API

```bash
uvicorn main:app --reload --port 8000
```

### 2. Run ingestion consumer

```bash
python data_consumer.py
```

Optional custom batch settings:

```bash
python data_consumer.py 50 20
```

Optional schema file:

```bash
python data_consumer.py 50 20 path/to/schema.json
```

### 3. One-command guided flow

```bash
python quickstart.py
```

## JSON CRUD Interface

Use pipeline directly:

```python
from ingestion_pipeline import IngestionPipeline

pipeline = IngestionPipeline()

# Insert
pipeline.execute_crud({
    "operation": "insert",
    "data": {
        "username": "alice",
        "email": "alice@example.com",
        "orders": [{"order_id": "o1", "item": "book", "quantity": 1, "price": 12.5}]
    }
})

# Read
result = pipeline.execute_crud({
    "operation": "read",
    "fields": ["username", "email", "orders"],
    "filters": {"username": "alice"}
})
print(result)

# Update (delete_then_insert)
pipeline.execute_crud({
    "operation": "update",
    "filters": {"username": "alice"},
    "data": {
        "username": "alice",
        "email": "alice+new@example.com",
        "orders": [{"order_id": "o2", "item": "phone", "quantity": 1, "price": 300.0}]
    }
})

# Delete
pipeline.execute_crud({
    "operation": "delete",
    "filters": {"username": "alice"}
})

pipeline.close()
```

## Tests

Run all tests:

```bash
pytest -q
```

Current coverage includes:
- schema registration
- buffer transition logic
- SQL normalization creation
- metadata-driven CRUD cycle
- generator endpoint and structure checks

## Documentation

See the docs folder:
- docs/ASSIGNMENT2_TECHNICAL_REPORT.md
- docs/ARCHITECTURE_ASSIGNMENT2.md
- docs/CRUD_JSON_INTERFACE.md

## Notes

- sys_ingested_at is the cross-backend join key.
- Update operation uses delete_then_insert for backend consistency.
- Buffer fields are persisted and later resolved when evidence is sufficient.
