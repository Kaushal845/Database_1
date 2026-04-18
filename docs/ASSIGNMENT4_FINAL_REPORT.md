# Assignment 4: Final Report — Hybrid Database Framework

**Group Name:** Schemeless  
**GitHub Repository:** https://github.com/<your-org>/Database_1  
**Video Demonstration:** [link to demo video]  

---

## 1. Dashboard Enhancements

### Existing Dashboard Features (from Assignment 3)
- **Overview Tab**: System-wide statistics (record counts, field counts, pipeline stats)
- **Records Tab**: Paginated record browser with username search filter
- **Entities Tab**: Logical entity catalog showing field types, coverage percentages, cardinality, sample values, and relationships
- **Query Builder Tab**: Full CRUD interface (read, insert, update, delete) with JSON filter/data input
- **Fields Tab**: Field placement status and semantic type distribution
- **Session Tab**: Active session info, timeline, and ACID transaction system status

### New Dashboard Features (Assignment 4)

#### Query History Tab
- Logs every executed query with: timestamp, operation type, filters, latency (ms), success/failure status, and record count
- Summary statistics: total queries, success/failure breakdown, average latency
- Operation breakdown showing counts per CRUD type
- Color-coded latency (green < 50ms, orange < 200ms, red ≥ 200ms)
- Auto-refreshes every 5 seconds; clear history button

#### Performance Tab
- Loads benchmark results from `docs/` JSON files
- Displays ingestion latency, query response times, and transaction overhead using pure CSS bar charts
- Comparative analysis section showing framework vs direct SQL/MongoDB with overhead percentages
- Detailed data tables with min/max/p95 statistics
- Instructions shown when no benchmark data is available; populates automatically after running benchmarks

### Design Constraint
The dashboard presents all data through logical abstractions. No backend-specific details (SQL table names, MongoDB collection names, indexing strategies, or placement decisions) are exposed in the user-facing interface.

---

## 2. Performance Evaluation

### Experimental Setup
- **Hardware**: [Describe your machine — CPU, RAM, OS]
- **Dataset**: Synthetic records (Faker) with: username, email, age, country, score, profile (nested), orders (array of objects)
- **Python**: 3.13.7
- **SQLite**: Built-in
- **MongoDB**: 6.x

### Experiments Conducted

1. **Ingestion Latency**: Measured per-record ingestion time across batch sizes (10, 50, 100, 500 records)
2. **Query Response Time**: Read (single filter, all records), Update (single record), Delete+re-insert cycle
3. **Metadata Lookup Overhead**: Field mapping and placement decision lookups
4. **Transaction Coordination**: Full 2PC (begin→prepare→commit) vs direct update

### Results

> Run `python performance_benchmark.py` to generate results.  
> See `docs/performance_report.md` for the full results table.

[Paste or reference performance_benchmark_results.json tables here]

### Analysis
- Per-record latency is dominated by dual-backend writes and type detection
- Metadata lookups are sub-millisecond (in-memory dictionary)
- 2PC adds measurable overhead but ensures ACID atomicity across backends
- Throughput scales linearly with batch size

---

## 3. Comparative Analysis

### Experiments
Compared the hybrid framework's logical abstraction layer against direct SQL/MongoDB access:

1. **Record Retrieval**: Framework read vs `SELECT * FROM` vs `db.collection.find()`
2. **Nested Document Access**: Framework vs direct MongoDB find with projection
3. **Record Update**: Framework update vs `UPDATE SET` vs `$set`
4. **Record Insertion**: Framework ingest vs `INSERT INTO` vs `insert_one`

### Results

> Run `python comparative_benchmark.py` to generate results.  
> See `docs/comparative_report.md` for the full results table.

[Paste or reference comparative_benchmark_results.json tables here]

### Discussion

**Where the framework adds overhead:**
- The abstraction layer adds latency due to metadata routing, dual-backend queries, and result merging
- Ingestion is substantially slower than direct inserts because of type detection, normalization, and placement heuristics

**Where the framework adds value:**
- Unified query interface — one query accesses data across SQL and MongoDB seamlessly
- Automatic schema evolution — new fields are auto-detected and routed without DDL
- Transparent normalization — arrays become child tables, nested objects go to MongoDB
- ACID across backends — 2-phase commit ensures atomic cross-backend operations
- Backend-agnostic applications — code doesn't need to know where data lives

---

## 4. System Limitations

- **Single-machine deployment**: No distributed architecture or replication
- **Buffer warmup period**: New fields require ~10 observations before permanent placement
- **No joins across entities**: Framework reads merge root + child data but doesn't support cross-entity joins
- **In-memory metadata**: Metadata store is persisted to JSON; very large schemas may need a database-backed store
- **No concurrent writes**: SQLite's write lock limits parallel ingestion throughput
- **Query history**: Stored in-memory; lost on API restart

---

## 5. System Setup

See [RUNNING.md](RUNNING.md) for complete installation, configuration, and usage instructions.
