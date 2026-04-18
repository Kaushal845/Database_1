# Hybrid Database Dashboard + ACID Validation System

A comprehensive React-based dashboard and ACID validation suite for a hybrid SQL (SQLite) + MongoDB database system with full transaction coordination.

## Overview

This project implements:

1. **React Dashboard** - Interactive visualization of database state
   - Real-time statistics from SQL and MongoDB
   - Paginated record viewer with field merging
   - Query builder for CRUD operations
   - Field placement visualization

2. **Transaction Coordinator** - ACID compliance across hybrid backends
   - 2-phase commit protocol
   - Atomic operations spanning SQL + MongoDB
   - Full rollback capability
   - Transaction state tracking

3. **ACID Test Suite** - Academic-grade validation
   - **Atomicity**: All-or-nothing execution (3 tests)
   - **Consistency**: Constraint enforcement (3 tests)
   - **Isolation**: Concurrent transaction handling (3 tests)
   - **Durability**: Crash recovery and persistence (3 tests)

## Architecture

```
┌─────────────────────┐
│  React Dashboard    │
│  (Port 3000)        │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  FastAPI Backend    │
│  (Port 8000)        │
├─────────────────────┤
│ • Dashboard API     │
│ • Transaction API   │
│ • Query Engine      │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ Transaction         │
│ Coordinator         │
└─────┬───────────┬───┘
      │           │
      ↓           ↓
 ┌────────┐  ┌─────────┐
 │  SQL   │  │ MongoDB │
 │(SQLite)│  │         │
 └────────┘  └─────────┘
```

## Installation

### Prerequisites

- Python 3.8+
- Node.js 16+ and npm
- MongoDB (optional - in-memory fallback available)

### Python Dependencies

```bash
pip install fastapi uvicorn faker sse-starlette pymongo requests pytest httpx
```

### React Dashboard Dependencies

```bash
cd dashboard
npm install
```

## Quick Start

### 1. Start the Backend API

```bash
python dashboard_api.py
```

The API will be available at `http://localhost:8000`

### 2. Start the React Dashboard

In a new terminal:

```bash
cd dashboard
npm run dev
```

The dashboard will be available at `http://localhost:3000`

### 3. Run ACID Tests

In a new terminal:

```bash
python acid_test_suite.py
```

This will:
- Run all 12 ACID validation tests
- Generate comprehensive reports in `docs/`:
  - `ACID_VALIDATION_REPORT.md` (Markdown)
  - `ACID_VALIDATION_REPORT.html` (HTML with styling)
  - `acid_validation_results.json` (Raw data)

## API Endpoints

### Dashboard Endpoints

- `GET /api/dashboard/summary` - Overall statistics
- `GET /api/dashboard/records` - Paginated record view
- `GET /api/dashboard/field-placements` - Field routing info
- `POST /api/query` - Execute CRUD operations

### Transaction Endpoints

- `POST /api/transaction/begin` - Start new transaction
- `POST /api/transaction/{tx_id}/operation` - Add operation
- `POST /api/transaction/{tx_id}/prepare` - Prepare transaction
- `POST /api/transaction/{tx_id}/commit` - Commit transaction
- `POST /api/transaction/{tx_id}/abort` - Rollback transaction
- `GET /api/transaction/{tx_id}/status` - Get transaction status
- `GET /api/transaction/list` - List active transactions

## Transaction Usage Example

```python
from transaction_coordinator import TransactionCoordinator
from database_managers import SQLManager, MongoDBManager

# Initialize
sql_mgr = SQLManager('my_data.db')
mongo_mgr = MongoDBManager()
coordinator = TransactionCoordinator(sql_mgr, mongo_mgr)

# Begin transaction
tx_id = coordinator.begin_transaction()

# Add operations
coordinator.add_operation(tx_id, 'insert', 'both', {
    'username': 'test_user',
    'sys_ingested_at': '2026-03-25T12:00:00.001',
    't_stamp': '2026-03-25T12:00:00',
    'email': 'test@example.com'
})

# Prepare (validate and execute)
success, error = coordinator.prepare(tx_id)

if success:
    # Commit changes
    coordinator.commit(tx_id)
else:
    # Rollback on failure
    coordinator.abort(tx_id)
```

## ACID Test Suite

### Atomicity Tests

- **A1**: Single insert failure rollback
- **A2**: Batch insert partial failure
- **A3**: Update with nested data failure

### Consistency Tests

- **C1**: Unique constraint enforcement
- **C2**: Foreign key integrity
- **C3**: Type constraint enforcement

### Isolation Tests

- **I1**: No dirty reads
- **I2**: Concurrent inserts
- **I3**: Serializable isolation

### Durability Tests

- **D1**: Crash recovery
- **D2**: Committed data persists
- **D3**: Durability after rollback

### Running Individual Tests

```python
from acid_test_suite import AcidTestSuite

suite = AcidTestSuite()

# Run specific test
result = suite.test_a1_single_insert_failure_rollback()
print(f"Test: {result.test_name}")
print(f"Passed: {result.passed}")
print(f"Evidence: {result.evidence}")
```

## Dashboard Features

### Overview Tab

- Real-time record counts (SQL/MongoDB/Buffer)
- Field distribution statistics
- Database object listing
- Pipeline metrics

### Records Tab

- Paginated record viewer
- Filter by username
- Expandable field details
- Merged view of SQL + MongoDB data

### Query Builder Tab

- Operation selector (read/insert/update/delete)
- Field selection
- JSON filter builder
- JSON data input
- Results visualization with query plan

### Field Placements Tab

- All field mappings
- Backend routing decisions (SQL/MongoDB/Both/Buffer)
- MongoDB strategy (embed/reference)
- Field statistics and semantic types

## File Structure

```
.
├── dashboard/                      # React frontend
│   ├── src/
│   │   ├── components/
│   │   │   ├── Dashboard.jsx      # Overview dashboard
│   │   │   ├── Records.jsx        #Record viewer
│   │   │   ├── QueryBuilder.jsx   # CRUD interface
│   │   │   └── FieldPlacements.jsx # Field routing
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── index.css
│   ├── package.json
│   └── vite.config.js
│
├── dashboard_api.py                # FastAPI backend
├── transaction_coordinator.py      # Transaction management
├── acid_test_suite.py              # ACID validation tests
├── acid_report_generator.py        # Test report generation
├── query_engine.py                 # CRUD query engine
├── database_managers.py            # SQL/MongoDB managers
├── ingestion_pipeline.py           # Data ingestion logic
├── metadata_store.py               # Field metadata tracking
├── placement_heuristics.py         # Backend routing decisions
└── docs/
    └── ACID_VALIDATION_REPORT.md   # Generated test report
```

## Technical Details

### Transaction Coordinator

Implements a 2-phase commit protocol:

1. **BEGIN**: Create transaction context, start SQL transaction with savepoint
2. **PREPARE**: Validate operations, track rollback info, stage changes
3. **COMMIT**: Finalize all changes atomically across both backends
4. **ABORT**: Rollback SQL via savepoint, clean MongoDB temp records

### Rollback Mechanism

- **SQL**: Uses savepoints for nested transaction support
- **MongoDB**: Tracks temporary records with `_tx_id` marker
- **Both**: Full state restoration on abort

### Limitations

- MongoDB standalone mode (not replica set) limits multi-document transaction support
- SQLite uses serialized transactions by default
- Crash simulation uses connection close rather than process kill
- Concurrent writes to the same record may conflict

## Development

### Running Tests

```bash
# ACID tests
python acid_test_suite.py

# Unit tests (if available)
pytest tests/

# Benchmark ingestion
python benchmark_ingestion.py
```

### Viewing Databases

```bash
# View current database state
python view_databases.py

# SQLite CLI
sqlite3 ingestion_data.db

# MongoDB shell
mongosh ingestion_db
```

