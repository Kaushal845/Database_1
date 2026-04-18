Back to [home](README.md)

# 3. Autonomous Normalization and CRUD Engine (Assignment 2)

## 3.1 Assignment objective

Assignment 2 extends basic placement into a full logical data engine:

- Normalize repeating JSON structures for SQL.
- Choose embed/reference strategies for nested Mongo content.
- Execute CRUD using metadata planning instead of hardcoded table/collection knowledge.

This chapter maps those requirements directly to implementation.

## 3.2 SQL normalization mechanics

Normalization signals extracted in `ingestion_pipeline.py`:

- Arrays of objects via `_extract_repeating_entities`.
- Arrays of primitives via `_extract_primitive_arrays`.
- Repeated scalar groups like `phone1`, `phone2`, `phone3` via `_extract_repeating_scalar_groups`.

Write path:

1. Determine target table name: `norm_{sanitized_entity_path}`.
2. Derive scalar columns from observed items.
3. Ensure child table exists through SQL manager.
4. Insert child rows with `parent_sys_ingested_at` foreign key.
5. Register normalization metadata (`register_normalized_table`).

This preserves one-to-many structures without bloating root rows.

## 3.3 Mongo embed vs reference strategy

`_compute_mongo_reference_score` computes a score per entity path using factors from both runtime shape and schema hints.

Signals include:

- Array size thresholds.
- Array-of-objects indicator.
- Object width.
- Nesting depth.
- Cross-parent sharing likelihood.
- Schema hints (`frequently_updated`, `shared`, `unbounded`, `expected_max_items`).

Decision rule implemented:

- score >= 2 -> `reference` mode (collection `ref_{entity_path}`).
- score < 2 -> `embed` mode (root collection).

Decision telemetry is persisted (score, reasons, threshold, hints), enabling later explanation and debugging.

## 3.4 Metadata model and persistence

`metadata_store.py` acts as persistent control-plane state for the whole framework.

Major sections:

- `fields`: appearances, type counts, samples, timestamps.
- `placement_decisions` and `current_placement`.
- `field_mappings`: backend + SQL table + Mongo collection + status.
- `schema_registry`: active schema + version history.
- `normalization.child_tables` and `mongo_strategy.entities`.
- `buffer.fields` and `quarantined_fields`.

Technical reliability feature:

- Metadata writes are atomic using temp file + move, reducing corruption risk on interruption.

## 3.5 CRUD planner internals

`MetadataDrivenQueryEngine` performs request-shape validation and operation dispatch.

### Read planning

`_build_field_plan` separates requested fields into:

- `sql_root_fields`
- `sql_child_entities`
- `mongo_root_fields`
- `mongo_reference_entities`

`_split_filters` creates backend-compatible filters and supports `$starts_with` for Mongo regex prefix matching.

Read execution behavior:

1. Query SQL root records first.
2. If SQL yields none, fallback to Mongo root for candidate keys.
3. Batch fetch Mongo root docs for all selected keys.
4. Batch fetch SQL child entities by table and parent key.
5. Batch fetch Mongo reference docs by collection + `entity_path`.
6. Merge by `sys_ingested_at` and return unified logical records.

Important optimization present in code:

- Child/reference fetches are batched per table/collection to reduce N+1 query overhead.

### Insert behavior

Insert delegates to ingestion callback so placement, normalization, and strategy logic are consistently reused.

Supports:

- Single record insert.
- List insert with inserted/failed counts.

### Update behavior

Update routing is mapping-aware and field-wise:

- SQL updates only if field maps to SQL and column exists (or can be created).
- Mongo updates use partial `$set` style semantics via manager update call.
- Unmapped fields are still eligible for Mongo (schemaless path).

This specifically addresses prior failure mode where MongoDB-only fields were incorrectly rejected during update.

### Delete behavior

Delete supports:

- Entity-specific deletion using mapping.
- Root-level deletion across SQL root, Mongo root, buffer, and reference collections.

Safety guard:

- Unfiltered delete is blocked unless explicit `allow_all=true`.
