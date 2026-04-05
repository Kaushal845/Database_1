# Autonomous Hybrid Ingestion Framework (Assignment 2)

Group Name: Schemeless  
Members: Shardul Junagade, Kaushal Bule, Akash Gupta

This project is a metadata-driven hybrid database framework. It accepts JSON records, learns the structure over time, decides where each piece should be stored (SQL, MongoDB, or Buffer), and then uses that same metadata to run CRUD operations.

If terms like normalization, embedding, and referencing are confusing right now, this README is written exactly for that.

## What Problem This Solves

In real systems, JSON payloads are not always fixed. Some fields are simple scalars, some are nested objects, some are arrays, and schema evolves.

If you force everything into SQL, nested data becomes awkward.  
If you force everything into Mongo, relational querying and constraints become weaker.

So this project uses a hybrid strategy:

- SQL for stable relational patterns.
- MongoDB for nested/flexible structures.
- Buffer as a temporary holding zone when confidence is low.
- Metadata as the "brain" that remembers decisions and drives future writes/reads.

## Concepts Explained Like a Beginner

### 1) What is Normalization?

Normalization means splitting repeating data into separate tables so the root table does not become huge or duplicated.

Example:

```json
{
    "username": "u1",
    "orders": [
        {"order_id": "o1", "item": "book"},
        {"order_id": "o2", "item": "bag"}
    ]
}
```

Instead of storing both orders inside one SQL row, we create:

- a root row in `ingested_records`
- child rows in `norm_orders`
- each child row links back with `parent_sys_ingested_at`

Benefits:

- cleaner schema
- one-to-many relationship support
- easier querying and indexing

### 2) What is Embed vs Reference in MongoDB?

MongoDB stores JSON-like documents.

Embed:

- nested data is stored inside the same root document
- good when nested data is small and usually read together

Reference:

- nested data is stored in a separate collection
- root stores link context, and child docs store `parent_sys_ingested_at`
- good for large arrays, frequently updated nested data, or shared-like entities

In this project, a score determines embed/reference automatically.

### 3) What is Metadata-Driven?

Metadata means data about data. Here it stores things like:

- field frequency and type stability
- where each field is stored
- normalized child table mapping
- Mongo strategy and decision reasons

Metadata-driven means:

- reads are planned from metadata (not hardcoded field rules)
- deletes/updates use metadata mappings
- previously buffered fields can be migrated when decisions finalize

### 4) What is Buffer?

Some fields appear too few times early in ingestion. Instead of making premature schema decisions, those values are staged in `buffer_records`.

When enough evidence is collected:

- field gets final placement (SQL/Mongo/Both)
- buffered values are drained to final destination
- buffer entries are cleaned up

## Assignment-2 Requirements and How This Repo Meets Them

From [assgns/assignment-2.md](assgns/assignment-2.md):

1. Automated SQL normalization.
2. Mongo document decomposition (embed vs reference).
3. Metadata-driven CRUD query generation.

Implemented in this repo:

1. SQL normalization for arrays of objects, primitive arrays, repeated scalar groups.
2. Score-based Mongo embed/reference strategy with persisted decision telemetry.
3. Query engine that builds plans from metadata and merges SQL + Mongo data into one JSON response.

## End-to-End Runtime Flow (What Happens Internally)

### Step 1: Optional schema registration

You may register schema hints (like `frequently_updated`, `unbounded`) that influence decisions later.

### Step 2: Ingestion starts

For each record, [ingestion_pipeline.py](ingestion_pipeline.py) does:

1. Track field stats recursively.
2. Add timestamps (`t_stamp`, `sys_ingested_at`).
3. Route scalar fields by heuristics to SQL/Mongo/Buffer/Both.
4. Detect repeating entities for SQL normalization.
5. Apply Mongo embed/reference strategy for nested fields.
6. Persist unresolved fields to buffer.

### Step 3: SQL writes

- root scalar row goes to `ingested_records`
- normalized child rows go to tables like `norm_orders`, `norm_tags`, `norm_phone`
- child rows keep `item_index` for ordering and `parent_sys_ingested_at` for joins

### Step 4: Mongo writes

- root document in `ingested_records`
- nested fields either embedded there or written to `ref_<entity>` collections

### Step 5: Metadata updates

[metadata_store.py](metadata_store.py) records:

- placement decisions
- field mappings
- normalization map
- mongo strategy and score reasons
- buffer state

### Step 6: CRUD uses metadata

[query_engine.py](query_engine.py) uses metadata to:

- build field plan
- split filters for SQL vs Mongo
- execute backend-specific queries
- merge by `sys_ingested_at`

## Detailed Decision Logic

### Placement Heuristics (SQL vs MongoDB vs Buffer vs Both)

In [placement_heuristics.py](placement_heuristics.py):

- mandatory keys (`username`, `sys_ingested_at`, `t_stamp`) -> Both
- structural types (`dict`, `list`) -> MongoDB directly
- low observations (<10) -> Buffer
- enough observations -> confidence-based SQL/Mongo decision
- drift handling can downgrade SQL to MongoDB

### Mongo Reference Score

In [ingestion_pipeline.py](ingestion_pipeline.py), score adds points based on:

- array size
- array-of-objects signal
- object width
- nesting depth
- likely shared entity
- schema hints (`frequently_updated`, `shared`, `unbounded`, `expected_max_items`)

Decision:

- score >= 2 -> reference
- score < 2 -> embed

Telemetry is stored with reasons so decisions are explainable.

## CRUD Behavior in Plain Language

### Insert

- Uses the same ingestion pipeline logic.
- Can accept one record or a list of records.

### Read

- Builds a query plan from metadata.
- Queries SQL root first.
- Falls back to Mongo root when SQL result is empty.
- Hydrates SQL child tables and Mongo reference collections.
- Returns merged JSON.

### Update

- Uses `delete_then_insert` strategy for consistency across both backends.

### Delete

- Can delete root records or a specific entity.
- Removes matching SQL rows, Mongo docs, and buffer docs as applicable.

## Example: One Record Through the System

Input:

```json
{
    "username": "demo_user",
    "email": "demo@example.com",
    "tags": ["new", "sale"],
    "orders": [
        {"order_id": "o1", "item": "book", "quantity": 1, "price": 10.0}
    ]
}
```

What happens:

1. Root scalar fields like `username`, `email` are classified.
2. `tags` (primitive array) becomes SQL child table `norm_tags`.
3. `orders` (array of objects) becomes SQL child table `norm_orders`.
4. `orders` also goes through Mongo embed/reference scoring.
5. Metadata stores all mappings.
6. Later read request for `username,email,orders,tags` is reconstructed from both backends.

## Important Files and Responsibilities

- [ingestion_pipeline.py](ingestion_pipeline.py): Orchestrates end-to-end ingestion and routing.
- [metadata_store.py](metadata_store.py): Persistent metadata brain.
- [placement_heuristics.py](placement_heuristics.py): Storage decision logic.
- [database_managers.py](database_managers.py): SQL/Mongo managers and operations.
- [query_engine.py](query_engine.py): Metadata-driven CRUD planning and execution.
- [tests/test_assignment2_pipeline.py](tests/test_assignment2_pipeline.py): Assignment-2 behavior validation.
- [quickstart.py](quickstart.py): Setup + clean command + guided run.
- [benchmark_ingestion.py](benchmark_ingestion.py): One-batch benchmark runner.
- [docs/ingestion_benchmark_results.json](docs/ingestion_benchmark_results.json): Measured benchmark output.

## Run Guide

### 1) Setup environment

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Start generator API

```bash
uvicorn main:app --reload --port 8000
```

### 3) Ingest data

```bash
python data_consumer.py
```

### 4) Run tests

```bash
pytest -q
```

### 5) Inspect state

```bash
python view_databases.py
python view_databases.py placements
python view_databases.py normalization
python view_databases.py mongo_strategy
python view_databases.py buffer
```

### 6) Clean generated artifacts

```bash
python quickstart.py clean --yes
```

## Benchmark Snapshot

Measured single-batch results (from [docs/ingestion_benchmark_results.json](docs/ingestion_benchmark_results.json)):

| Batch Size | Elapsed (s) | Per Record (ms) | Throughput (rec/s) |
|---:|---:|---:|---:|
| 10 | 3.3421 | 334.211 | 2.99 |
| 20 | 16.0346 | 801.729 | 1.25 |
| 50 | 23.4952 | 469.903 | 2.13 |
| 100 | 47.9602 | 479.602 | 2.09 |
| 500 | 277.1893 | 554.379 | 1.80 |
| 1000 | 538.6599 | 538.660 | 1.86 |

## Limits and Trade-offs

- Embed/reference threshold is heuristic and static today.
- Update is full rewrite (`delete_then_insert`), which is consistent but can be expensive.
- Buffer can temporarily hold unresolved fields until observation confidence grows.

## Team Contributions

- Shardul: Code implementation
- Akash Gupta: Report writing
- Kaushal: Research work

## Where to Read More

- [assgns/assignment-2.md](assgns/assignment-2.md): Official assignment specification.
- [docs/ASSIGNMENT2_TECHNICAL_REPORT.md](docs/ASSIGNMENT2_TECHNICAL_REPORT.md): Technical markdown report.
- [reports/Schemeless_A2.tex](reports/Schemeless_A2.tex): Final LaTeX report.
- [README_FILES_ADDED.md](README_FILES_ADDED.md): File-by-file list of Assignment-2 added/majorly-updated files.
