# Running the Hybrid Database Framework

Complete instructions to install, configure, and run the entire system.

## Prerequisites

| Dependency | Version | Purpose |
|-----------|---------|---------|
| **Python** | 3.11+ | Backend, ingestion pipeline, benchmarks |
| **Node.js** | 18+ | Dashboard frontend (React + Vite) |
| **MongoDB** | 6.0+ | NoSQL backend (must be running locally) |

> SQLite is included with Python — no separate installation needed.

---

## 1. Clone the Repository

```bash
git clone https://github.com/Kaushal845/Database_1.git
cd Database_1
```

## 2. Python Virtual Environment

```bash
# Create and activate virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## 3. Ensure MongoDB Is Running

MongoDB must be running on `localhost:27017` (default port, no authentication).

```bash
# Check if MongoDB is running
mongosh --eval "db.adminCommand('ping')"
```

If not running, start MongoDB:
- **Windows**: `net start MongoDB` or start from Services
- **macOS**: `brew services start mongodb-community`
- **Linux**: `sudo systemctl start mongod`

## 4. Start the Backend API (Assignment 4 Integrated Mode)

```bash
# From the project root
.venv\Scripts\python.exe dashboard_api.py
```

The API starts on **http://localhost:8000**. Health check: http://localhost:8000/health

This initializes:
- SQLite database (`ingestion_data.db`)
- MongoDB connection (`hybrid_ingestion_db`)
- Metadata store (`metadata_store.json`)
- Transaction coordinator (ACID support)

## 5. Start the Generator API (Assignment 1/2 Compatible Mode)

Use this mode when you want the original ingestion API flow used in earlier assignments.
Do not run this together with Section 4 at the same time, since both use port 8000.

```bash
# From the project root (in the same virtual environment)
uvicorn main:app --reload --port 8000
```

## 6. Start the Dashboard Frontend

```bash
# In a separate terminal
cd dashboard
npm install       # First time only
npm run dev
```

Dashboard opens on **http://localhost:3000**

### Dashboard Tabs

| Tab | Description |
|-----|-------------|
| **Overview** | System stats: record counts, field counts, pipeline stats |
| **Records** | Browse and search ingested records with pagination |
| **Entities** | Inspect logical entities, their fields, types, and coverage |
| **Query Builder** | Execute CRUD operations (read, insert, update, delete) |
| **Query History** | View executed queries with latency, status, and filters |
| **Performance** | Visualize benchmark results with bar charts and tables |
| **Fields** | View field placement status and semantic types |
| **Session** | Session timeline, activity stats, transaction status |

## 7. Ingest Sample Data

```bash
# Quick test — ingest a small batch
.venv\Scripts\python.exe quickstart.py

# Or use the data consumer for larger datasets
.venv\Scripts\python.exe data_consumer.py

# Quickstart with detailed logs
.venv\Scripts\python.exe quickstart.py --verbose

# Optional batch settings
.venv\Scripts\python.exe data_consumer.py 50 20
.venv\Scripts\python.exe data_consumer.py 50 20 path/to/schema.json
```

View ingested data and metadata with:

```bash
.venv\Scripts\python.exe view_databases.py
```

## 8. Run Benchmarks

```bash
# Performance benchmark (ingestion latency, query times, metadata overhead, transaction overhead)
.venv\Scripts\python.exe performance_benchmark.py

# Comparative benchmark (framework vs direct SQL/MongoDB access)
.venv\Scripts\python.exe comparative_benchmark.py
```

Results are saved to `docs/`:
- `docs/performance_benchmark_results.json`
- `docs/performance_report.md`
- `docs/comparative_benchmark_results.json`
- `docs/comparative_report.md`

After running benchmarks, the **Performance** tab in the dashboard will show the results.

## 9. Run Tests

```bash
.venv\Scripts\python.exe -m pytest tests/ -v
```

## 10. Clean Generated Files

```bash
.venv\Scripts\python.exe quickstart.py clean --yes
```

This cleans local artifacts and also drops project MongoDB databases:
- `ingestion_db`
- `assignment2_test_db`

Keep MongoDB data (files/cache only):

```bash
.venv\Scripts\python.exe quickstart.py clean --no-mongo
```

## 11. Build Dashboard for Production

```bash
cd dashboard
npm run build
```

The production build is output to `dashboard/dist/` and is served by the backend API at the root URL.

## 12. Assignment Documentation

- `docs/ASSIGNMENT2_TECHNICAL_REPORT.md`
- `docs/ARCHITECTURE_ASSIGNMENT2.md`
- `docs/CRUD_JSON_INTERFACE.md`
- `docs/ASSIGNMENT4_FINAL_REPORT.md`
- `docs/performance_report.md`
- `docs/comparative_report.md`

---

## Directory Structure

```
Database_1/
├── dashboard_api.py          # FastAPI backend (port 8000)
├── ingestion_pipeline.py     # Core ingestion + CRUD orchestrator
├── query_engine.py           # Metadata-driven query execution
├── transaction_coordinator.py # 2-phase commit across SQL + MongoDB
├── database_managers.py      # SQL and MongoDB connection managers
├── metadata_store.py         # Field tracking and placement decisions
├── placement_heuristics.py   # Automatic field placement rules
├── type_detector.py          # Automatic type detection
├── data_consumer.py          # Batch data ingestion client
├── performance_benchmark.py  # Performance evaluation suite
├── comparative_benchmark.py  # Framework vs direct access comparison
├── dashboard/                # React + Vite frontend
│   └── src/components/       # Dashboard UI components
├── tests/                    # Pytest test suite
├── docs/                     # Reports and benchmark results
├── assgns/                   # Assignment specifications
└── reports/                  # PDF reports
```
