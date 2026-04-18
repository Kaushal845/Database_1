Back to [home](README.md)

# 6. Assignment Requirements Mapping (A1-A4)

This chapter maps assignment expectations to concrete implementation areas in this repository.

## 6.1 Assignment 1 mapping

| Requirement theme | Implemented behavior | Primary modules |
|---|---|---|
| Adaptive ingestion of heterogeneous JSON | Recursive field tracking before write; schema-free ingest path | `ingestion_pipeline.py`, `type_detector.py` |
| Type drift handling | Type distribution tracked per field; drift-aware placement decisions | `metadata_store.py`, `placement_heuristics.py` |
| SQL vs Mongo placement | Rule-based routing with mandatory dual keys and structural fast-path | `placement_heuristics.py`, `ingestion_pipeline.py` |
| Traceability across split backends | `sys_ingested_at` and `t_stamp` propagation | `ingestion_pipeline.py` |
| Deferred uncertainty handling | Buffer storage + migration drain when field is resolved | `ingestion_pipeline.py`, `metadata_store.py` |

## 6.2 Assignment 2 mapping

| Requirement theme | Implemented behavior | Primary modules |
|---|---|---|
| Automatic normalization | Repeating entities converted to SQL child tables `norm_*` | `ingestion_pipeline.py`, `database_managers.py` |
| Mongo embed/reference strategy | Score-based strategy with threshold and telemetry | `ingestion_pipeline.py`, `metadata_store.py` |
| Metadata-driven CRUD | Logical operation planning from field mappings and strategy metadata | `query_engine.py`, `metadata_store.py` |
| CRUD coverage | Insert/read/update/delete execution over both backends | `query_engine.py` |
| Schema registration/versioning | Active schema + version history persisted | `metadata_store.py` |

## 6.3 Assignment 3 mapping

| Requirement theme | Implemented behavior | Primary modules |
|---|---|---|
| ACID validation concept | 2PC-style coordinated begin/prepare/commit/abort | `transaction_coordinator.py` |
| Atomic cross-backend writes | SQL savepoints + staged Mongo temp operations | `transaction_coordinator.py`, `query_engine.py` |
| Logical dashboard | API + React dashboard views for session/entity/records/query | `dashboard_api.py`, `dashboard/src/components/*` |
| Query monitoring | Query execution history endpoint and UI surface | `dashboard_api.py`, `QueryHistory.jsx` |
| No backend detail leakage in logical views | Logical labels and merged JSON outputs | dashboard UI + API summary layers |

## 6.4 Assignment 4 mapping

| Requirement theme | Implemented behavior | Primary modules |
|---|---|---|
| Performance evaluation | Ingestion, query, metadata, transaction, throughput benchmarks | `performance_benchmark.py` |
| Comparative analysis | Framework vs direct SQL/Mongo tests for reads, nested access, updates, inserts | `comparative_benchmark.py` |
| Visualization support | Benchmark artifacts consumed by dashboard performance view | `PerformanceView.jsx`, `docs/*.json` |
| Packaging/deployment | Dockerfile + Compose + setup guide | `Dockerfile`, `docker-compose.yml`, `docs/DOCKER_SETUP.md` |
| Reproducibility | Generated JSON reports and markdown reports in `docs` | benchmark scripts + report outputs |

## 6.5 Evidence artifacts

Runtime and report evidence available in repository:

- `docs/performance_benchmark_results.json`
- `docs/comparative_benchmark_results.json`
- `docs/performance_report.md`
- `docs/comparative_report.md`
- `docs/ASSIGNMENT4_FINAL_REPORT.md`
