# 📹 HYBRID DATABASE SYSTEM - DEMO SCRIPTS

This directory contains demo scripts used for the video demonstration of the hybrid SQL+MongoDB database system.

## Demo Files Overview

### 1. `demo_setup.py`
**Purpose:** Data ingestion and field placement demonstration
**Shows:**
- Ingesting 5 sample records with diverse field types
- Field placement decisions (SQL vs MongoDB vs Both)
- Cross-backend query execution
- Placement reasoning and statistics

**Run:**
```bash
python demo_setup.py
```

**Expected Output:**
```
DATA INGESTION & FIELD PLACEMENT
[Sample records ingested]
FIELD PLACEMENT ANALYSIS
[Placement decisions shown]
CROSS-BACKEND QUERY EXAMPLE
[Results merged from both backends]
```

---

### 2. `demo_2pc_transactions.py`
**Purpose:** Two-Phase Commit protocol demonstration
**Shows:**
- Successful transaction (both backends commit)
- Failed transaction (automatic rollback)
- 2PC phases: PREPARE and COMMIT
- Atomicity guarantee (all-or-nothing)

**Run:**
```bash
python demo_2pc_transactions.py
```

**Expected Output:**
```
SCENARIO 1: Successful Transaction Across Both Backends
[T0] BEGIN TRANSACTION
[T1] PHASE 1: PREPARE
[T2] PREPARE COMMIT
[T3] Both backends prepared successfully!
[T4] PHASE 2: COMMIT
[T5] TRANSACTION SUCCESSFUL

SCENARIO 2: Failure in Commit Phase - Automatic Rollback
[Shows automatic rollback on MongoDB failure]

2PC PROTOCOL SUMMARY
[Summary of guarantees]
```

---

### 3. `demo_acid_tests.py`
**Purpose:** ACID property validation
**Shows:**
- Atomicity: All-or-nothing execution
- Consistency: Constraint enforcement
- Isolation: Read isolation
- Durability: Write-ahead logging

**Run:**
```bash
python demo_acid_tests.py
```

**Expected Output:**
```
TEST 1: ATOMICITY - Multi-Backend Insert Success
[Validation of atomic writes]

TEST 2: CONSISTENCY - Referential Integrity
[Validation of constraint enforcement]

TEST 3: ISOLATION - Dirty Read Prevention
[Validation of transaction isolation]

TEST 4: DURABILITY - Write-Ahead Logging
[Validation of data persistence]

ACID VALIDATION SUMMARY
✓ ALL ACID TESTS PASSED
```

---

### 4. `demo_dashboard_verify.py`
**Purpose:** Dashboard component verification
**Shows:**
- Session information display
- Entity catalog data
- Field placement visualizations
- Query builder functionality
- Backend consistency checks

**Run:**
```bash
python demo_dashboard_verify.py
```

**Expected Output:**
```
DASHBOARD COMPONENT VERIFICATION

SESSION INFO
[Connection status and record counts]

ENTITY CATALOG
[List of entities and instances]

FIELD PLACEMENTS
[Field distribution across backends]

QUERY BUILDER
[Query 1 and Query 2 results]

BACKEND CONSISTENCY CHECK
[Data consistency verification]

✓ ALL DASHBOARD COMPONENTS VERIFIED
```

---

### 5. `demo_run_all.py`
**Purpose:** Master orchestration script - runs all demos
**Shows:**
- Complete system verification
- Summary report of all components
- Overall pass/fail status

**Run:**
```bash
python demo_run_all.py
```

**Expected Output:**
```
ENVIRONMENT VERIFICATION
[Checks Python, OS, dependencies]

STEP 2: RUNNING DEMO COMPONENTS
[1/4] Data Setup
[2/4] 2PC Transactions
[3/4] ACID Tests
[4/4] Dashboard Verification

VERIFICATION SUMMARY
Total Tests: 4/4
Passed: 4/4
Pass Rate: 100%

✓ ALL COMPONENTS VERIFIED - SYSTEM READY FOR VIDEO DEMO
```

---

## Quick Start

### Option 1: Run All Demos (Recommended)
```bash
python demo_run_all.py
```

This will run all 4 demo components and provide a summary report. Expect 2-3 minutes total execution time.

### Option 2: Run Individual Demos
```bash
# Setup and data ingestion
python demo_setup.py

# Transaction coordination
python demo_2pc_transactions.py

# ACID validation
python demo_acid_tests.py

# Dashboard verification
python demo_dashboard_verify.py
```

---

## System Requirements

### Python Dependencies
```bash
pip install pymongo fastapi uvicorn faker requests
```

### External Services
- **MongoDB:** Should be running (or use in-memory fallback)
- **SQLite:** Built-in (no installation needed)

### Verify Setup
```bash
python -c "
from database_managers import SQLManager, MongoDBManager
sql = SQLManager()
mongo = MongoDBManager()
print('✓ All systems ready')
sql.close()
mongo.close()
"
```

---

## Demo Database Artifacts

The demo scripts create the following database files:

- **SQL:** `demo_ingestion.db` (SQLite database)
- **MongoDB:** `demo_ingestion_db` (collections in MongoDB)
- **Metadata:** `demo_metadata.json` (field placement decisions)

To reset/clean databases:
```bash
rm demo_ingestion.db demo_metadata.json
# MongoDB: use `mongo drop database demo_ingestion_db`
```

---

## Troubleshooting

### MongoDB Connection Error
```
Error: [Errno 111] Connection refused
```
**Solution:** Start MongoDB
```bash
mongod --dbpath /data/db
```

Or use in-memory fallback (automatically used if MongoDB unavailable)

### SQLite Lock Error
```
Error: database is locked
```
**Solution:** Close other connections to the database
```bash
rm *.db-wal *.db-shm  # Remove WAL files
```

### Import Errors
```
ModuleNotFoundError: No module named 'pymongo'
```
**Solution:** Install dependencies
```bash
pip install pymongo fastapi uvicorn faker requests
```

---

## For Video Recording

### Recommended Recording Setup

1. **Terminal 1:** Run `python demo_run_all.py` for full verification
2. **Browser:** Open dashboard at `http://localhost:5173` (after starting frontend)
3. **Code Editor:** Show relevant source files

### Recording Duration
- Each individual demo: 30-60 seconds
- Complete run: 2-3 minutes
- Final summary: 30 seconds

### Capture Tips
- Use `asciinema` for terminal recording: `asciinema rec`
- Use OBS Studio for multi-window capture
- Zoom to 125% for better readability on video
- Ensure good lighting for camera views

---

## Demo Output Files

All scripts generate clear, timestamped output suitable for video:
- ✓/✗ indicators for pass/fail
- Progress indicators [T0], [T1], [T2], etc.
- Clear section dividers with ASCII borders
- Human-readable timing information
- JSON data structures for technical viewers

---

## Video Script Integration

These demos support the following video scenes:

| Scene | Demo | Duration |
|-------|------|----------|
| Scene 3: Data Ingestion | `demo_setup.py` | 2:30 |
| Scene 6: 2PC Demo | `demo_2pc_transactions.py` | 3:00 |
| Scene 7: ACID Tests | `demo_acid_tests.py` | 3:30 |
| Scene 4: Dashboard | `demo_dashboard_verify.py` | 3:00 |
| **Total** | `demo_run_all.py` | **12:00** |

---

## Command Line Examples

### Run with Verbose Output
```bash
python demo_run_all.py --verbose
```

### Run Single Component with Timing
```bash
time python demo_setup.py
```

### Clean and Restart
```bash
rm demo_*.db demo_metadata.json
python demo_run_all.py
```

---

## Next Steps

After verifying demos:

1. ✓ Start the React Dashboard
2. ✓ Start FastAPI backend
3. ✓ Run video recording with demos
4. ✓ Edit video with captions and background music
5. ✓ Upload to course platform

---

## Support

For issues or questions about demo scripts:
- Check individual script docstrings: `python -c "import demo_setup; help(demo_setup)"`
- Review logging output
- Check database connectivity
- Verify all dependencies installed

---

**Last Updated:** April 5, 2026  
**System:** Hybrid SQL + MongoDB with Transaction Coordination  
**Status:** Ready for Video Demonstration ✓
