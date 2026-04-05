# Assignment-2 Files Added or Majorly Updated

This document lists the files added for Assignment-2 work, or files that were significantly upgraded to implement Assignment-2 requirements.

Note:
- Some files may have existed from Assignment-1 and were enhanced heavily in Assignment-2.
- Generated artifacts are listed separately at the end.

## Core Implementation Files

## [ingestion_pipeline.py](ingestion_pipeline.py)
Purpose:
- Main orchestrator of the Assignment-2 pipeline.
- Handles record ingestion, metadata tracking, scalar routing, normalization, Mongo embed/reference strategy, and buffer drain.

Key responsibilities:
- Recursive field tracking
- SQL normalization extraction
- Mongo scoring and strategy selection
- Buffer reconciliation at startup
- CRUD engine delegation

## [metadata_store.py](metadata_store.py)
Purpose:
- Persistent metadata manager (JSON-backed).

Key responsibilities:
- schema registry and versioning
- field stats and type distributions
- placement decisions and field mappings
- buffer state tracking
- normalization map
- mongo strategy and decision telemetry

## [placement_heuristics.py](placement_heuristics.py)
Purpose:
- Decision engine for SQL vs MongoDB vs Buffer vs Both.

Key responsibilities:
- mandatory field placement
- observation warm-up threshold
- confidence/zone-based routing
- drift handling and quarantine
- uniqueness/index hints

## [query_engine.py](query_engine.py)
Purpose:
- Metadata-driven CRUD execution layer.

Key responsibilities:
- insert/read/update/delete operation handling
- query plan generation from metadata mappings
- SQL/Mongo filter split
- merge SQL + Mongo results into unified JSON

## [database_managers.py](database_managers.py)
Purpose:
- SQL and Mongo operational adapters.

Key responsibilities:
- dynamic SQL column/table management
- normalized child-table insertion
- Mongo root/reference operations
- in-memory Mongo fallback when server unavailable

## Supporting Runtime Files

## [main.py](main.py)
Purpose:
- FastAPI entrypoint exposing ingestion/generator endpoints.

## [data_consumer.py](data_consumer.py)
Purpose:
- Pulls generated records and feeds them into the pipeline.

## [quickstart.py](quickstart.py)
Purpose:
- Guided local run script and cleanup command.

Notable Assignment-2 addition:
- `clean` flow removes generated files and optionally project Mongo databases.

## [view_databases.py](view_databases.py)
Purpose:
- Inspection utility to view placement, normalization, mongo strategy, and buffer diagnostics.

## Test and Validation Files

## [tests/test_assignment2_pipeline.py](tests/test_assignment2_pipeline.py)
Purpose:
- Assignment-2 pytest suite for behavior validation.

Coverage includes:
- schema registration
- normalization behavior
- buffer transition and drain
- metadata-driven CRUD cycle
- mongo strategy scoring and hints
- nested-field direct mongo routing

## Documentation and Report Files

## [assgns/assignment-2.md](assgns/assignment-2.md)
Purpose:
- Official assignment instructions and rubric.

## [docs/ARCHITECTURE_ASSIGNMENT2.md](docs/ARCHITECTURE_ASSIGNMENT2.md)
Purpose:
- Assignment-2 architecture explanation.

## [docs/ASSIGNMENT2_TECHNICAL_REPORT.md](docs/ASSIGNMENT2_TECHNICAL_REPORT.md)
Purpose:
- Markdown technical report for implementation details.

## [docs/CRUD_JSON_INTERFACE.md](docs/CRUD_JSON_INTERFACE.md)
Purpose:
- JSON API examples for insert/read/update/delete.

## [reports/Schemeless_A2.tex](reports/Schemeless_A2.tex)
Purpose:
- Final LaTeX report used for PDF submission.

## Benchmark Files

## [benchmark_ingestion.py](benchmark_ingestion.py)
Purpose:
- Executes one-batch benchmark runs across multiple batch sizes with isolated fresh state.

Output:
- writes results to [docs/ingestion_benchmark_results.json](docs/ingestion_benchmark_results.json)

## [docs/ingestion_benchmark_results.json](docs/ingestion_benchmark_results.json)
Purpose:
- Stores measured benchmark timings used in README/report.

## Generated Benchmark SQL Artifacts

These are generated during benchmarking and can be recreated:

- [docs/benchmark_ingestion_10.db](docs/benchmark_ingestion_10.db)
- [docs/benchmark_ingestion_20.db](docs/benchmark_ingestion_20.db)
- [docs/benchmark_ingestion_50.db](docs/benchmark_ingestion_50.db)
- [docs/benchmark_ingestion_100.db](docs/benchmark_ingestion_100.db)
- [docs/benchmark_ingestion_500.db](docs/benchmark_ingestion_500.db)
- [docs/benchmark_ingestion_1000.db](docs/benchmark_ingestion_1000.db)

## Team Roles

- Shardul: code implementation
- Akash Gupta: report writing
- Kaushal: research work
