# Autonomous Data Ingestion System

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **A self-learning data ingestion system that dynamically determines optimal storage backends (SQL vs MongoDB) for heterogeneous JSON streams without human intervention.**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Technical Report](#technical-report)

---

## 🎯 Overview

This system solves a critical challenge in modern data engineering: **autonomous data placement**. Unlike traditional ETL systems with predefined schemas, our system:

- **Learns** field patterns from incoming data
- **Adapts** storage decisions based on frequency and type stability
- **Handles** unclean, heterogeneous JSON records
- **Maintains** data integrity with bi-temporal timestamps
- **Persists** decisions across restarts

### The Problem

Real-world data streams are messy:
- Fields appear with different casings (`ip`, `IP`, `IpAddress`)
- Types drift (`battery: 50` vs `battery: "50%"`)
- Data is nested unpredictably
- Schemas are unknown in advance

### The Solution

An intelligent pipeline that:
1. **Normalizes** field names dynamically
2. **Tracks** field statistics (frequency, type stability)
3. **Decides** SQL vs MongoDB placement using heuristics
4. **Stores** data with traceability (username + dual timestamps)

---

## ✨ Features

### 🔄 Dynamic Field Normalization
- Resolves naming ambiguities (`userName` → `user_name`)
- Case-insensitive matching (`IP` → `ip`)
- Semantic equivalence rules (`emailAddress` → `email`)

### 📊 Intelligent Placement Heuristics
- **High frequency + stable type** → SQL
- **Low frequency or nested** → MongoDB
- **Mandatory fields** → Both (for joins)

### 🕐 Bi-Temporal Timestamps
- `t_stamp`: Client timestamp (from JSON)
- `sys_ingested_at`: Server timestamp (unique join key)

### 💾 Persistent Metadata
- Survives system restarts
- Tracks field statistics
- Stores placement decisions

### 🔍 Semantic Type Detection
- Distinguishes `"1.2.3.4"` (IP) from `1.2` (float)
- Recognizes UUIDs, emails, URLs, timestamps

### 📝 Comprehensive Reporting
- Answers all assignment questions
- Field-by-field analysis
- Placement justifications

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI Server                       │
│              (Synthetic Data Generator)                 │
└────────────────────┬────────────────────────────────────┘
                     │ JSON Stream
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  Data Consumer                          │
│              (data_consumer.py)                        │
└────────────────────┬────────────────────────────────────┘
                     │ Raw Records
                     ▼
┌─────────────────────────────────────────────────────────┐
│              Ingestion Pipeline                         │
│           (ingestion_pipeline.py)                       │
│                                                         │
│  1. Field Normalizer    → Resolve naming ambiguities   │
│  2. Type Detector       → Semantic type detection      │
│  3. Metadata Store      → Track frequency & stability  │
│  4. Placement Logic     → Decide SQL vs MongoDB        │
└────────────────────┬────────────────────────────────────┘
                     │
       ┌─────────────┴──────────────┐
       ▼                            ▼
┌─────────────┐            ┌──────────────┐
│ SQL Manager │            │ MongoDB Mgr  │
│  (SQLite)   │            │   (MongoDB)  │
└─────────────┘            └──────────────┘
```

---

## 📦 Installation

### Prerequisites

- Python 3.8 or higher
- MongoDB (optional, system works with SQL only if MongoDB unavailable)

### Step 1: Clone Repository

```bash
git clone https://github.com/YogeshKMeena/Course_Resources.git
cd Databases-1
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `fastapi` - API server for data generation
- `uvicorn` - ASGI server
- `faker` - Synthetic data generation
- `sse-starlette` - Server-Sent Events
- `pymongo` - MongoDB driver
- `requests` - HTTP client

### Step 3: Optional - Start MongoDB

If you have MongoDB installed:

```bash
# Windows
mongod

# Linux/Mac
sudo service mongodb start
```

**Note:** System will work with SQLite only if MongoDB is not available.

---

## 🚀 Quick Start

### 1. Start the Data Generator

Open a terminal and start the FastAPI server:

```bash
uvicorn main:app --reload --port 8000
```

You should see:
```
INFO:     Uvicorn running on http://127.0.0.1:8000
```

### 2. Run the Ingestion System

Open a **second terminal** and run the consumer:

```bash
python data_consumer.py
```

**Default Configuration:**
- Batch size: 100 records
- Total batches: 10
- Total records: 1,000

### 3. Custom Configuration

```bash
# Ingest 500 records in batches of 50
python data_consumer.py 50 10

# Ingest 10,000 records
python data_consumer.py 100 100
```

### 4. Generate Technical Report

After ingestion completes:

```bash
python report_generator.py
```

This creates `TECHNICAL_REPORT.md` with detailed analysis.

---

## 📖 Usage

### Basic Ingestion

```python
from ingestion_pipeline import IngestionPipeline

# Create pipeline
pipeline = IngestionPipeline()

# Ingest a single record
record = {
    "username": "john_doe",
    "email": "john@example.com",
    "age": 30,
    "ip_address": "192.168.1.1"
}
pipeline.ingest_record(record)

# Get statistics
stats = pipeline.get_statistics()
print(stats)

# Cleanup
pipeline.close()
```

### Batch Ingestion

```python
from ingestion_pipeline import IngestionPipeline

pipeline = IngestionPipeline()

records = [
    {"username": "user1", "email": "user1@test.com"},
    {"username": "user2", "Email": "user2@test.com"},  # Different casing
    {"userName": "user3", "metadata": {"nested": "value"}}  # Nested data
]

pipeline.ingest_batch(records)
pipeline.close()
```

### Custom Data Source

```python
from ingestion_pipeline import IngestionPipeline
import json

pipeline = IngestionPipeline()

# From file
with open('data.json', 'r') as f:
    for line in f:
        record = json.loads(line)
        pipeline.ingest_record(record)

# From API
import requests
response = requests.get('https://api.example.com/data')
for record in response.json():
    pipeline.ingest_record(record)

pipeline.close()
```

---

## 🔧 How It Works

### 1. Field Normalization

When a record arrives:

```python
# Input
{
    "userName": "john",
    "emailAddress": "john@test.com",
    "IP": "192.168.1.1"
}

# After normalization
{
    "username": "john",
    "email": "john@test.com",
    "ip_address": "192.168.1.1"
}
```

**Rules:**
1. Convert camelCase → snake_case
2. Lowercase all field names
3. Apply semantic equivalence patterns
4. Store mapping for consistency

### 2. Type Detection

Semantic analysis differentiates:

```python
"1.2.3.4"     → 'ip_address'  (IP pattern match)
1.2           → 'float'        (Python type)
"1.2"         → 'string'       (not IP, not float)
"user@x.com"  → 'email'        (email pattern)
```

### 3. Frequency & Type Stability Tracking

For each field:

```python
{
    'email': {
        'appearances': 950,          # Seen in 950 records
        'type_counts': {
            'email': 950             # Always email type
        },
        'frequency': 95.0,           # 95% of records
        'type_stability': 100.0      # 100% consistent type
    }
}
```

### 4. Placement Decision

**Decision Tree:**

```
Is field in [username, sys_ingested_at, t_stamp]?
  YES → Both backends
  NO  → Continue

Is field type 'dict' or 'list'?
  YES → MongoDB
  NO  → Continue

Frequency >= 60% AND Type Stability >= 80%?
  YES → SQL
  NO  → Continue

Frequency < 60%?
  YES → MongoDB
  NO  → Continue

Type Stability < 80%?
  YES → MongoDB
  NO  → MongoDB (default)
```

### 5. Data Splitting

```python
# Original record
{
    "username": "john",
    "email": "john@test.com",      # SQL (high freq, stable)
    "altitude": 150.5,              # MongoDB (low freq)
    "metadata": {"nested": "data"}, # MongoDB (nested)
    "sys_ingested_at": "2024-..."   # Both (mandatory)
}

# SQL record
{
    "username": "john",
    "email": "john@test.com",
    "sys_ingested_at": "2024-..."
}

# MongoDB record
{
    "username": "john",
    "altitude": 150.5,
    "metadata": {"nested": "data"},
    "sys_ingested_at": "2024-..."
}
```

### 6. Joining Data

```python
# Query SQL for structured data
sql_result = SELECT * FROM ingested_records WHERE username = 'john'

# Use sys_ingested_at to fetch full document from MongoDB
mongo_result = db.ingested_records.find({
    "sys_ingested_at": sql_result['sys_ingested_at']
})
```

---

## ⚙️ Configuration

### Placement Thresholds

Edit `placement_heuristics.py`:

```python
class PlacementHeuristics:
    FREQUENCY_THRESHOLD = 60.0        # Default: 60%
    TYPE_STABILITY_THRESHOLD = 80.0   # Default: 80%
    MIN_OBSERVATIONS = 10             # Default: 10 records
```

### Database Connections

Edit `ingestion_pipeline.py`:

```python
pipeline = IngestionPipeline(
    metadata_file='metadata_store.json',
    sql_db='ingestion_data.db',
    mongo_uri='mongodb://localhost:27017/',
    mongo_db='ingestion_db'
)
```

### API URL

Edit `data_consumer.py`:

```python
consumer = DataConsumer(api_url='http://127.0.0.1:8000')
```

---

## 🐛 Troubleshooting

### Issue: Cannot connect to API

**Error:**
```
[Consumer] ✗ Cannot connect to API
```

**Solution:**
1. Ensure FastAPI server is running:
   ```bash
   uvicorn main:app --reload --port 8000
   ```
2. Check if port 8000 is available:
   ```bash
   netstat -an | findstr 8000  # Windows
   lsof -i :8000               # Linux/Mac
   ```

### Issue: MongoDB connection failed

**Error:**
```
[MongoDB] Connection failed: ServerSelectionTimeoutError
```

**Solution:**
1. System continues with SQL only - this is expected behavior
2. To use MongoDB:
   ```bash
   # Windows
   mongod

   # Linux/Mac
   sudo service mongodb start
   ```

### Issue: Permission denied on database file

**Error:**
```
sqlite3.OperationalError: unable to open database file
```

**Solution:**
1. Ensure write permissions in current directory
2. Or specify absolute path:
   ```python
   pipeline = IngestionPipeline(sql_db='C:/path/to/ingestion_data.db')
   ```

### Issue: Field normalization not working

**Problem:** Same field creating multiple columns

**Solution:**
1. Check `metadata_store.json` for existing mappings
2. Delete metadata file to reset (only during testing):
   ```bash
   rm metadata_store.json
   ```
3. Restart ingestion

---

## 📁 Project Structure

```
Databases-1/
│
├── main.py                      # FastAPI data generator
├── data_consumer.py             # API client & ingestion orchestrator
├── ingestion_pipeline.py        # Main ingestion logic
├── metadata_store.py            # Persistent metadata storage
├── field_normalizer.py          # Field name normalization
├── type_detector.py             # Semantic type detection
├── placement_heuristics.py      # SQL vs MongoDB decision logic
├── database_managers.py         # SQL & MongoDB interfaces
├── report_generator.py          # Technical report generator
│
├── requirements.txt             # Python dependencies
├── readme.md                    # Original project README
├── README_SYSTEM.md             # This file
├── LICENSE                      # MIT License
│
├── metadata_store.json          # Generated: Persistent metadata
├── ingestion_data.db            # Generated: SQLite database
├── TECHNICAL_REPORT.md          # Generated: Assignment report
│
└── __pycache__/                 # Python cache
```

---

## 📊 Technical Report

After running the ingestion system, generate the comprehensive technical report:

```bash
python report_generator.py
```

The report (`TECHNICAL_REPORT.md`) includes:

1. **Normalization Strategy** - How field ambiguities were resolved
2. **Placement Heuristics** - Thresholds and decision logic
3. **Uniqueness Detection** - How unique fields were identified
4. **Value Interpretation** - Semantic type detection methodology
5. **Mixed Data Handling** - Type drifting strategies
6. **System Statistics** - Comprehensive metrics
7. **Field Analysis** - Per-field breakdown
8. **Architecture** - System design overview

---

## 🎓 Assignment Questions Answered

### Q1: How did you resolve type naming ambiguities?

✅ Multi-stage normalization:
- CamelCase → snake_case conversion
- Lowercase standardization
- Semantic equivalence patterns
- Persistent mapping storage

### Q2: What thresholds were used for placement decisions?

✅ Dynamic thresholds:
- Frequency: 60%
- Type Stability: 80%
- Min Observations: 10 records
- Priority-based decision tree

### Q3: How did you identify unique fields?

✅ Multi-factor analysis:
- Name-based heuristics (contains 'id', 'uuid', 'key')
- Type-based heuristics (UUID, integer IDs)
- Cardinality analysis (>90% unique values)

### Q4: How did you differentiate "1.2.3.4" (IP) from 1.2 (float)?

✅ Cascading type detection:
- Python type checking (int, float, bool)
- Regex pattern matching (IP, UUID, email)
- Validation (IP octets 0-255)
- Order matters: IP check before generic string

### Q5: How did you handle type drifting?

✅ Adaptive strategy:
- Track type occurrences per field
- Calculate type stability percentage
- Adjust placement when stability drops
- Store as TEXT in SQL, native types in MongoDB

---

## 📈 Example Output

### Console Output

```
====================================================================
AUTONOMOUS DATA INGESTION SYSTEM
====================================================================

[Pipeline] Initialized successfully
[Pipeline] Loaded 0 known fields
[Consumer] Testing connection to http://127.0.0.1:8000...
[Consumer] ✓ API server is reachable

[Consumer] Configuration:
  - Batch size: 100 records
  - Total batches: 10
  - Total records: 1,000
  - API URL: http://127.0.0.1:8000

[Consumer] === Batch 1/10 ===
[SQL] Added column: email (VARCHAR(255))
[SQL] Added column: age (INTEGER)
[Pipeline] Processed 50 records (SQL: 50, MongoDB: 50)
[Pipeline] Processed 100 records (SQL: 100, MongoDB: 100)

[Consumer] === Batch 2/10 ===
...

=== Final Statistics ===
Total records processed: 1000
SQL inserts: 1000
MongoDB inserts: 1000
Errors: 0
Unique fields discovered: 42
Normalization rules: 8
Placement decisions made: 42
```

### Database Contents

**SQLite:**
```sql
SELECT * FROM ingested_records LIMIT 3;

id | username  | sys_ingested_at        | t_stamp            | email          | age
---|-----------|------------------------|-------------------|----------------|----
1  | user123   | 2024-02-14T10:30:00.0  | 2024-02-14T10:29  | user@test.com  | 25
2  | alice456  | 2024-02-14T10:30:01.0  | 2024-02-14T10:29  | alice@test.com | 30
3  | bob789    | 2024-02-14T10:30:02.0  | 2024-02-14T10:29  | bob@test.com   | 35
```

**MongoDB:**
```javascript
db.ingested_records.find().limit(1).pretty()

{
    "_id": ObjectId("..."),
    "username": "user123",
    "sys_ingested_at": "2024-02-14T10:30:00.0",
    "t_stamp": "2024-02-14T10:29",
    "altitude": 542.26,
    "metadata": {
        "sensor_data": {
            "version": "2.1",
            "calibrated": false,
            "readings": [10, 8, 10]
        },
        "tags": ["out"],
        "is_bot": false
    }
}
```

---

## 🤝 Contributing

This project is part of a database coursework assignment. For questions or issues:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review the generated `TECHNICAL_REPORT.md`
3. Examine `metadata_store.json` for system state

---

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **Course:** Database Systems
- **Data Generator:** Based on Course_Resources by YogeshKMeena
- **Assignment:** Autonomous Data Ingestion System

---

## 📞 Support

For technical questions about the implementation:

1. Read this README thoroughly
2. Generate and review `TECHNICAL_REPORT.md`
3. Check metadata file: `metadata_store.json`
4. Review module docstrings

---

**Built with ❤️ for Database Systems Course**

*Last Updated: February 2026*
