# Comparative Analysis Report

Generated: 2026-04-18T12:02:46

## Summary

| Test | Framework (ms) | Direct SQL (ms) | Direct MongoDB (ms) | Overhead |
|------|---------------|----------------|--------------------:|----------|
| Record Retrieval (single filter read) | 2.40 | 0.02 | 1.16 | 103.0% |
| Nested Document Access (orders + profile) | 3.36 | N/A | 1.49 | 126.1% |
| Record Update (single field) | 8.81 | 0.02 | 1.20 | 619.0% |
| Record Insertion | 366.77 | 0.10 | 1.03 | 32214.4% |

## Record Retrieval (single filter read)

Framework overhead: **103.0%**

| Method | Avg (ms) | Min (ms) | Max (ms) | P95 (ms) |
|--------|---------|---------|---------|---------|
| Framework (logical query) | 2.403 | 1.885 | 3.899 | 3.088 |
| Direct SQL (SELECT) | 0.023 | 0.020 | 0.080 | 0.025 |
| Direct MongoDB (find) | 1.161 | 0.705 | 1.800 | 1.561 |

## Nested Document Access (orders + profile)

Framework overhead: **126.1%**

| Method | Avg (ms) | Min (ms) | Max (ms) | P95 (ms) |
|--------|---------|---------|---------|---------|
| Framework (logical query) | 3.362 | 2.233 | 5.619 | 4.853 |
| Direct MongoDB (find + projection) | 1.487 | 1.131 | 3.045 | 2.028 |

## Record Update (single field)

Framework overhead: **619.0%**

| Method | Avg (ms) | Min (ms) | Max (ms) | P95 (ms) |
|--------|---------|---------|---------|---------|
| Framework (logical update) | 8.815 | 6.511 | 16.013 | 11.075 |
| Direct SQL (UPDATE) | 0.022 | 0.014 | 0.209 | 0.017 |
| Direct MongoDB ($set) | 1.204 | 0.914 | 1.627 | 1.582 |

## Record Insertion

Framework overhead: **32214.4%**

| Method | Avg (ms) | Min (ms) | Max (ms) | P95 (ms) |
|--------|---------|---------|---------|---------|
| Framework (full pipeline) | 366.768 | 274.711 | 1237.725 | 372.490 |
| Direct SQL (INSERT) | 0.103 | 0.067 | 0.270 | 0.182 |
| Direct MongoDB (insert_one) | 1.032 | 0.560 | 1.645 | 1.567 |

## Discussion

### Where the abstraction adds overhead

- **Read operations**: The framework performs metadata lookups, routes queries to both SQL and MongoDB, then merges results. This adds measurable latency compared to a single direct query.
- **Ingestion**: The framework runs type detection, placement heuristics, normalization, and dual-backend writes — significantly slower than a single raw INSERT or insert_one.
- **Updates**: The framework routes fields to the correct backend based on metadata, updating SQL and MongoDB separately. Direct updates on a single backend are naturally faster.

### Where the abstraction provides value

- **Unified access**: Users query a single logical interface without knowing which backend stores each field. This eliminates the need to write separate SQL and MongoDB queries.
- **Automatic schema evolution**: New fields are automatically detected, classified, and routed to the optimal backend without manual schema changes.
- **Nested entity management**: The framework handles normalization of arrays into child tables, embedded MongoDB documents, and reference-based entities — all transparently.
- **ACID guarantees**: The 2-phase commit ensures atomic operations across both backends, which would require custom implementation with direct access.
- **Backend-agnostic applications**: Application code doesn't need to change if data moves between backends (e.g. during schema evolution or optimization).

### When to use the framework vs direct access

| Scenario | Recommended Approach |
|----------|---------------------|
| Schema-flexible, evolving data | Framework |
| High-throughput, fixed-schema writes | Direct backend |
| Cross-backend queries (SQL + MongoDB) | Framework |
| Single-backend, latency-critical reads | Direct backend |
| Need ACID across SQL + MongoDB | Framework |
