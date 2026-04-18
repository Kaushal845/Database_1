Back to [home](README.md)

# 1. Introduction and Global Architecture

## 1.1 Problem this system solves

The framework addresses a common practical conflict:

- Relational engines are excellent for consistent scalar fields and table-style analytics.
- Document engines are excellent for nested and evolving payloads.
- Real JSON streams contain both at once.

Instead of forcing users to pick one model, this project implements a logical database layer where users send and receive JSON while the framework decides storage placement, executes cross-backend operations, and reconstructs unified records.

## 1.2 Assignment progression (A1 to A4)

- Assignment 1: adaptive ingestion and placement.
- Assignment 2: normalization, Mongo strategy, and metadata-driven CRUD.
- Assignment 3: ACID-style transaction coordination and logical dashboard.
- Assignment 4: performance/comparative analysis and deployable packaging.

The code reflects this incremental design. New responsibilities are layered on top of previous ones rather than replacing them.

## 1.3 Runtime architecture

Primary orchestrator: `ingestion_pipeline.py` (`IngestionPipeline`).

Core collaborators:

- `type_detector.py`: semantic type inference.
- `placement_heuristics.py`: backend decision logic.
- `metadata_store.py`: persistent runtime metadata and schema versions.
- `database_managers.py`: SQL/Mongo persistence primitives.
- `query_engine.py`: metadata-driven logical CRUD planner/executor.
- `transaction_coordinator.py`: two-phase-like coordination for write operations.
- `dashboard_api.py`: FastAPI contract for dashboard and API consumers.

## 1.4 End-to-end data lifecycle

1. Record arrives from generator or API.
2. Pipeline records recursive field statistics before mutation.
3. Pipeline injects temporal keys:
   - `t_stamp` (client-visible event time, preserved or derived).
   - `sys_ingested_at` (server-side unique record key).
4. Scalar and structural fields are routed independently.
5. SQL root insert executes first; then SQL child normalization writes.
6. Mongo root embed/reference writes execute.
7. Unresolved fields are parked in `buffer_records` and later drained.
8. Metadata updates after each decision and is atomically saved to JSON.

## 1.5 Logical data contract

The query contract in `query_engine.py` accepts operations:

- `insert`
- `read`
- `update`
- `delete`

A request can target logical fields without backend details. Planning uses `field_mappings`, `normalization`, and `mongo_strategy` from metadata.

## 1.6 Keys and join strategy

The framework uses `sys_ingested_at` as the universal join spine:

- SQL root table stores it as a unique field.
- SQL child tables store `parent_sys_ingested_at` foreign keys.
- Mongo root docs store it directly.
- Mongo reference collections store `parent_sys_ingested_at` plus `entity_path`.

This allows deterministic reconstruction even when one logical record spans SQL rows, Mongo root docs, and Mongo reference docs.

## 1.7 Metadata as system memory

`metadata_store.py` stores:

- Schema versions (`schema_registry`).
- Field observations and semantic type counts (`fields`).
- Placement decisions and reasons (`placement_decisions`, `current_placement`).
- Query routing map (`field_mappings`).
- SQL normalization map (`normalization.child_tables`).
- Mongo mode decisions with scoring telemetry (`mongo_strategy.entities`).
- Quarantine and buffer state (`quarantined_fields`, `buffer.fields`).

Without this metadata, query routing and explainability would not be possible.

## 1.8 API and UI boundary

`dashboard_api.py` runs as the integration boundary:

- Serves dashboard assets from `dashboard/dist` at `/`.
- Exposes monitoring endpoints (summary, records, entities, fields, session).
- Exposes query endpoint for logical CRUD.
- Maintains in-memory query history (capped list).
- Keeps compatibility endpoints used by ingestion scripts (`/schema`, `/record/{count}`).

The UI in `dashboard/src/components` is intentionally logical. It reports session/entity/query concepts, not physical table/collection internals.

## 1.9 Deployment architecture

Docker path:

- `docker-compose.yml` starts `mongodb` and `app` containers.
- `app` exposes FastAPI + built dashboard at port 8000.
- Mongo URI host is network-aware (`mongodb` service hostname).

Local path:

- SQLite file + local MongoDB + Python process.
- Same API contract and logical behavior as Docker mode.

## 1.10 Design trade-off summary

Benefits:

- Backend-agnostic API surface.
- Automatic schema evolution and placement.
- ACID-style coordination across heterogeneous stores.

Costs:

- Extra CPU and I/O for routing, metadata management, and merge logic.
- Higher latency than direct single-backend operations.

Assignment 4 benchmarks quantify these costs and are covered in Chapter 5.