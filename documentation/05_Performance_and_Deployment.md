Back to [home](README.md)

# 5. Performance Evaluation and Deployment (Assignment 4)

## 5.1 Assignment objective

Assignment 4 requires:

- quantitative performance evaluation,
- comparative framework-vs-direct access analysis,
- dashboard enhancements for visibility,
- reproducible deployment packaging.

This chapter uses the executable benchmark suites and generated reports already present in `docs`.

## 5.2 Benchmark implementation architecture

Two benchmark executables implement the required analysis:

- `performance_benchmark.py`
- `comparative_benchmark.py`

Both create isolated benchmark datasets and metadata files per run and output:

- JSON artifact files for machine-readable results.
- markdown reports for human review.

## 5.3 Performance evaluation coverage (framework-internal)

`performance_benchmark.py` measures six categories:

1. Ingestion latency across batch sizes (10, 50, 100, 500).
2. Query latency for read/update/delete-cycle operations.
3. Metadata lookup overhead.
4. Transaction coordination overhead.
5. Data distribution across SQL/Mongo/buffer.
6. Throughput under increasing workload.

Methodologically, it uses repeated timed calls with avg/min/max/p95 output for latency-centric tests.

## 5.4 Comparative analysis coverage (framework vs direct)

`comparative_benchmark.py` executes A/B comparisons for:

1. Record retrieval: framework logical read vs direct SQL + direct Mongo find.
2. Nested document access: framework vs direct Mongo projection.
3. Record update: framework update vs direct SQL update vs direct Mongo `$set`.
4. Record insertion: full framework ingest vs direct SQL insert vs direct Mongo insert.

Overhead is computed per scenario from measured averages.

## 5.5 Observed benchmark results (from generated reports)

From `docs/performance_report.md`:

- Ingestion average latency is roughly 314 to 367 ms/record as batch size grows.
- Read latency stays low (about 1.7 ms single-filter, about 3.0 ms broader read).
- Metadata lookups are near-zero (about 0.0001 ms average in report).
- Full 2PC path is measured in low single-digit milliseconds in this environment.
- Field distribution example in report shows Mongo-dominant field placement with both-backend keys retained.

From `docs/comparative_report.md`:

- Framework read overhead is about 103% vs direct baseline in tested case.
- Nested access overhead is about 126%.
- Update overhead is about 619%.
- Insertion overhead is very high (about 32214%) because framework insertion includes full ingest pipeline behavior (classification, metadata, normalization, dual writes), not a raw single-backend insert.

Interpretation:

- The framework intentionally trades raw latency for abstraction, schema evolution, and multi-backend correctness.
- Overhead is expected and is part of Assignment 4's trade-off analysis requirement.

## 5.6 Dashboard enhancements tied to Assignment 4

The dashboard includes Assignment 4 visibility features:

- Query history view with operation, latency, status, and counts.
- Performance view that reads benchmark artifacts and renders comparative metrics.
- Preservation of logical abstraction in general UI sections.

The API layer also serves built dashboard assets directly, simplifying one-port deployment and TA demonstration flow.

## 5.7 Deployment and packaging model

Primary packaging path is Docker Compose.

`docs/DOCKER_SETUP.md` defines operational flow:

1. Build/start containers.
2. Verify health endpoint.
3. Trigger ingestion.
4. View dashboard and docs endpoint.
5. Run benchmark suites inside container.

Persistence model documented in setup guide:

- Mongo data persisted via named volume.
- Reports/artifacts persisted by mounting `docs` to host.
- SQL/metadata persistence depends on path configuration and mounted targets.

