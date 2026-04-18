## Main System Files

- **main.py**
  - FastAPI server for Assignment-2 synthetic data generation
  - Emits nested and repeating entities for normalization/document strategy tests
  - Endpoints: `/`, `/record/{count}`, `/schema`, `/health`

- **data_consumer.py**
  - Client for consuming generator stream and sending to ingestion pipeline
  - Supports optional schema registration and CRUD query passthrough

- **ingestion_pipeline.py**
  - Assignment-2 core orchestrator
  - Implements schema registration, classification, buffer routing, normalization, Mongo strategy, and CRUD entrypoint

- **query_engine.py**
  - Metadata-driven CRUD query generator and executor
  - Converts JSON operations into SQL/Mongo actions and merges responses

- **metadata_store.py**
  - Persistent metadata manager
  - Tracks schema versions, field mappings, placement decisions, buffer state, normalization map, and Mongo strategy map

- **type_detector.py**
  - Detects semantic type of each field value (e.g., IP, UUID, float, string)
  - Features: Pattern matching, type coercion, SQL type mapping

- **placement_heuristics.py**
  - Decision engine for routing fields to SQL, MongoDB, Buffer, or Both
  - Uses frequency, type stability, confidence, and drift rules

- **database_managers.py**
  - SQL and MongoDB data access managers
  - SQL: dynamic root schema + normalized child-table support with FK constraints
  - Mongo: multi-collection operations with in-memory fallback

- **view_databases.py**
  - Utility for inspecting contents of SQL and MongoDB databases
  - Features: Query, print, and summarize ingested data

- **quick_test.py**
  - Streamlined script for rapid, non-interactive system testing
  - Features: API connectivity check, batch ingestion, summary output

---

## Supporting/Test Files

- **test_sql.py**
  - Unit tests for SQL backend (schema, insert, query, constraints)

- **test_mongodb.py**
  - Unit tests for MongoDB backend (connection, insert, query)

- **tests/test_assignment2_pipeline.py**
  - Pytest suite for Assignment-2 pipeline behavior
  - Covers schema registration, buffer transitions, normalization, and CRUD cycle

- **tests/test_generator.py**
  - Pytest coverage for generator endpoints and emitted Assignment-2 structures

- **pytest.ini**
  - Pytest discovery configuration (restricts collection to tests folder)

- **readme.md**
  - Detailed Assignment-2 documentation

- **README_SHORT.md**
  - Short run instructions

- **LICENSE**
  - MIT License for open source use

---

## Generated/Runtime Files

- **metadata_store.json**
  - Persistent metadata (schema registry, mappings, normalization, placement, buffer, strategies)

---

## Docs Folder

- **docs/ASSIGNMENT2_TECHNICAL_REPORT.md**
  - Mandatory report-question coverage for Assignment-2

- **docs/ARCHITECTURE_ASSIGNMENT2.md**
  - Pipeline architecture and design notes

- **docs/CRUD_JSON_INTERFACE.md**
  - JSON request/response reference for metadata-driven CRUD

