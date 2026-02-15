## Main System Files

- **main.py**
  - FastAPI server for synthetic data generation (JSON records)
  - Used for testing and simulating real-world data streams
  - Features: REST API, batch endpoints, realistic/nested data

- **data_consumer.py**
  - Client for consuming data from the FastAPI server
  - Orchestrates ingestion pipeline and batch processing
  - Features: API connectivity, batch ingestion, progress reporting

- **ingestion_pipeline.py**
  - Core orchestration logic for data ingestion
  - Normalizes fields, detects types, updates metadata, routes to SQL/MongoDB
  - Features: Autonomous placement, normalization, type detection, statistics

- **metadata_store.py**
  - Persistent storage for field statistics, normalization rules, and placement decisions
  - Features: Tracks frequency, type stability, normalization mappings, and more

- **field_normalizer.py**
  - Handles field name normalization and semantic equivalence
  - Prevents duplicate columns due to naming variations
  - Features: Regex-based normalization, persistent mapping

- **type_detector.py**
  - Detects semantic type of each field value (e.g., IP, UUID, float, string)
  - Features: Pattern matching, type coercion, SQL type mapping

- **placement_heuristics.py**
  - Decision engine for routing fields to SQL, MongoDB, or both
  - Uses frequency and type stability thresholds
  - Features: Rule-based placement, adaptive to data drift

- **database_managers.py**
  - Interfaces for SQL (SQLite) and MongoDB backends
  - Handles schema evolution, inserts, and indexing
  - Features: Dynamic schema, unique constraints, flexible storage

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

- **readme.md**
  - Main project documentation (features, setup, usage, architecture)

- **LICENSE**
  - MIT License for open source use

---

## Generated/Runtime Files

- **metadata_store.json**
  - Persistent metadata (field stats, normalization, placement)

