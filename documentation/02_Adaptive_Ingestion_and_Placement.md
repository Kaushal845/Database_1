Back to [home](README.md)

# 2. Adaptive Ingestion and Storage Placement (Assignment 1)

## 2.1 Assignment objective and implemented scope

Assignment 1 required an autonomous ingestion system that handles heterogeneous JSON and decides backend placement without hardcoded per-field routing.

Implemented in this repository:

- Recursive field observation before write.
- Semantic type detection (not just Python primitive checks).
- Placement with explainable reasoning.
- Mandatory dual-write keys for traceability.
- Deferred handling of unresolved fields and migration when resolved.

## 2.2 Ingestion entry and temporal guarantees

`IngestionPipeline.ingest_record` performs an explicit sequence:

1. Clone input (`enriched_record = dict(raw_record)`).
2. Track field statistics for all recursive paths.
3. Inject temporal identifiers with `_add_temporal_timestamps`.

Temporal design:

- `sys_ingested_at` is generated server-side and made unique by suffixing an increment-based sequence fragment.
- `t_stamp` is preserved if provided; otherwise derived from `timestamp` or current UTC.

This ensures each logical record has both event-time and ingest-time context while preserving a stable cross-backend join key.

## 2.3 Type detection behavior

`type_detector.py` classifies values into semantic categories:

- Structural: `dict`, `list`
- Primitive: `boolean`, `integer`, `float`, `string`, `null`
- Semantic strings: `ip_address`, `uuid`, `email`, `url`, `timestamp`

Why this matters:

- Placement heuristics use dominant type and stability.
- SQL schema evolution maps semantic types to storage-friendly SQL types.
- ID-like and semantic fields can be candidates for stricter handling.

## 2.4 Placement engine details

`placement_heuristics.py` uses layered rules rather than one threshold.

Decision priority:

1. Mandatory keys (`username`, `sys_ingested_at`, `t_stamp`) -> `Both`.
2. Structural dominant type (`dict`, `list`) -> `MongoDB` immediately.
3. Observation warm-up and zone-based confidence checks.
4. Booster promotion for SQL candidates.
5. Drift-based downgrade/quarantine.

Key mechanics:

- Frequency zones: high/medium/low.
- Stability zones: stable/moderate/unstable.
- Confidence score from frequency, stability, and semantic signal.
- Booster signals include semantic type, uniqueness likelihood, low null ratio.
- Drift responses:
	- Moderate drift: SQL to MongoDB downgrade.
	- Severe drift: quarantine marker in metadata.

All decisions are persisted with reasoning text so routing can be audited.

## 2.5 Scalar vs structural routing

In `ingestion_pipeline.py`, scalar top-level fields flow through `_route_scalar_field`:

- Adds SQL columns dynamically when needed.
- Writes to SQL, Mongo, both, or unresolved buffer based on placement.
- Updates `field_mappings` with backend and status (`final` or `buffer`).

Structural fields (objects/lists) flow through `_apply_mongo_document_strategy` and are not treated like scalar columns.

## 2.6 Buffer behavior and reconciliation

Unresolved fields are stored in Mongo collection `buffer_records` with:

- `sys_ingested_at`
- `username`
- `fields` object containing unresolved key/value payloads

When a field becomes resolved, `_drain_buffered_field` migrates historical buffered values into final backend targets, then removes the buffered key from old docs.

Startup reconciliation:

- `_reconcile_resolved_buffer_fields_on_startup` scans old buffered fields and drains any with finalized non-buffer decisions.

This avoids permanent data loss in early uncertain placement phases.

## 2.7 Mandatory joinability rule

The assignment required records remain linkable after split storage. Implemented by forcing `username`, `sys_ingested_at`, and `t_stamp` into both paths where relevant.

Operational effect:

- Read reconstruction can always pivot by `sys_ingested_at`.
- Entity-level and root-level updates/deletes can apply consistent filters.

