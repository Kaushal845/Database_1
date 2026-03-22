# Assignment 2 Technical Report

## 1. Normalization Strategy
The SQL normalization strategy is metadata-driven and automatic:
- The ingestion pipeline recursively scans each JSON record for repeating entities.
- A repeating entity is detected when a field is an array of objects, for example orders, comments, or devices.
- Primitive arrays such as tags: ["new", "sale"] are normalized into value child tables.
- Repeated scalar groups such as phone1, phone2, phone3 are collapsed into one normalized entity table.
- For each repeating entity path, the pipeline creates a normalized child table named using the entity path, such as norm_orders.
- Scalar keys inside each object become child-table columns.
- Each child row is linked to the root row using parent_sys_ingested_at.

This approach handles one-to-many decomposition without predefined relational schema files.

## 2. Table Creation Logic (PK/FK/Indexes)
Root table:
- ingested_records
- Primary key: id (autoincrement)
- Natural join key: sys_ingested_at (unique)

Child tables:
- id as primary key
- parent_sys_ingested_at foreign key references ingested_records(sys_ingested_at)
- ON DELETE CASCADE for root-level delete propagation
- item_index column preserves source order in arrays
- Index on parent_sys_ingested_at for efficient parent-child fetch

Field-level SQL columns in root are still created dynamically with semantic type mapping.

## 3. MongoDB Design Strategy (Embed vs Reference)
The Mongo strategy engine evaluates nested structures per field path using a score-based decision model:
- Embed mode:
  - Small objects
  - Short arrays
  - Content with low expected rewrite cost
- Reference mode:
  - Larger or unbounded arrays of objects
  - Broader objects likely to change independently
  - Shared entities across multiple parent contexts

Scoring inputs include:
- array size and object width
- deep nesting level
- likely shared-entity patterns from observed metadata
- schema hints such as frequently_updated, shared, unbounded, and expected_max_items

Nested structural fields (dict/list) bypass Buffer warm-up and route directly to MongoDB.

For reference mode, payloads are written to dedicated collections such as ref_orders and linked using parent_sys_ingested_at.

## 4. Metadata System
The metadata store persists all routing intelligence and structure mappings:
- schema_registry:
  - Active schema
  - Version history
- fields:
  - Appearance counts
  - Type distributions
  - Sample values
- placement_decisions:
  - SQL, MongoDB, Buffer, Both
  - Decision reason and timestamp
- buffer:
  - Undecided field observations
  - Resolution tracking
- field_mappings:
  - Backend placement
  - SQL table mapping
  - Mongo collection mapping
- normalization:
  - Child tables and entity-path mapping
- mongo_strategy:
  - Embed/reference decision and collection mapping
  - Decision telemetry: decision_score, reference_threshold, decision_reasons, schema_hints

This metadata drives ingestion and query generation uniformly.

## 5. CRUD Query Generation
A metadata-driven query engine supports JSON CRUD requests.

Insert:
- Operation payload is passed through pipeline ingestion.
- Record is split and routed by field mapping.

Read:
- Requested fields are resolved to SQL root, SQL child tables, Mongo root docs, and Mongo reference collections.
- SQL rows and Mongo docs are merged on sys_ingested_at.
- Child entities are attached as arrays in the output JSON.

Update:
- Implemented using delete_then_insert strategy for consistency across both backends.

Delete:
- Root delete removes SQL root rows and Mongo root docs.
- SQL child rows cascade via FK.
- Mongo referenced entities are removed via mapped collections.

## 6. Performance Considerations
Design choices to reduce complexity and rewrite overhead:
- Parent-key index on normalized child tables.
- SQL cascade deletion avoids expensive manual child scans.
- Metadata-driven field selection avoids querying both backends for every field.
- Mongo referencing limits large-document rewrites for expanding arrays.
- Buffer staging delays costly schema/index churn on low-observation fields.
- Structural nested fields skip Buffer warm-up, reducing temporary buffer writes for clearly document-shaped entities.

## 7. Sources of Information
- SQLite documentation:
  - Foreign keys and ON DELETE CASCADE
  - Dynamic ALTER TABLE behavior
- MongoDB documentation:
  - Embedding vs references modeling guidance
  - Collection indexing fundamentals
- FastAPI documentation:
  - API endpoint design and validation patterns
- Course assignment specification and architecture note files in assgns folder
