# Autonomous Data Ingestion System

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> **A self-learning data ingestion system that dynamically determines optimal storage backends (SQL vs MongoDB) for heterogeneous JSON streams without human intervention.**

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Data Generator API](#data-generator-api)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Project Structure](#project-structure)
- [Technical Report](#technical-report)

---

##  Overview

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

##  System Architecture

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

##  Data Generator API

The system includes a FastAPI-based synthetic data generator (`main.py`) that produces realistic JSON records for testing and demonstration purposes. This component operates independently and can be used for various data ingestion experiments.

### Features

- **Realistic Data Generation**: Uses Faker library to generate authentic-looking records
- **Randomized Fields**: Each record contains a mix of required and optional fields
- **Missing/Null Fields**: Simulates real-world data quality issues
- **Batch Streaming**: Support for single or multiple record retrieval
- **Offline Operation**: Works without external API dependencies
- **Nested Structures**: Generates hierarchical JSON with metadata objects and arrays

### API Endpoints

**Root Endpoint** - Single random record:
```
GET http://127.0.0.1:8000/
```

**Batch Endpoint** - Multiple records:
```
GET http://127.0.0.1:8000/record/{count}
```
Example: `http://127.0.0.1:8000/record/100` returns 100 records

**Health Check**:
```
GET http://127.0.0.1:8000/health
```

### Sample Generated Record

```json
{
  "username": "sloantimothy",
  "name": "Veronica Williamson",
  "ip_address": "22.2.183.169",
  "device_model": "OnePlus 12",
  "app_version": "v1.5.5",
  "altitude": 542.26,
  "country": "Cook Islands",
  "postal_code": "74596",
  "session_id": "473af720-92e2-4c52-9825-db272121d36d",
  "steps": 11994,
  "spo2": 98,
  "sleep_hours": 4.1,
  "weather": "stormy",
  "temperature_c": 23.1,
  "item": null,
  "payment_status": "pending",
  "language": "Akan",
  "cpu_usage": 21,
  "is_active": true,
  "avatar_url": "https://picsum.photos/652/342",
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

### Field Types Generated

**Always Present:**
- `username` - Unique identifier for traceability
- `timestamp` - Client-side timestamp

**Frequently Present (~60-95%):**
- Health metrics: `spo2`, `heart_rate`, `sleep_hours`
- Location: `country`, `timezone`, `ip_address`
- Device info: `device_model`, `app_version`
- User activity: `steps`, `purchase_value`, `session_id`

**Occasionally Present (~10-50%):**
- Optional metrics: `altitude`, `battery`, `disk_usage`
- Social data: `friends_count`, `comment`, `email`
- System info: `cpu_usage`, `ram_usage`, `error_code`

**Nested Structures:**
- `metadata.sensor_data` - Nested object with arrays
- `metadata.tags` - Array of strings
- Various deeply nested optional fields

### Starting the Data Generator

```bash
# Install dependencies
pip install fastapi uvicorn faker sse-starlette

# Start the server
uvicorn main:app --reload --port 8000
```

The API will be available at `http://127.0.0.1:8000` with interactive docs at `http://127.0.0.1:8000/docs`.

---

##  Installation

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

## Quick Start

### Method 1: Quick Test (Recommended for First Run)

The fastest way to test the system:

**Step 1:** Start the FastAPI server in one terminal:
```bash
uvicorn main:app --reload --port 8000
```

**Step 2:** Run the quick test in another terminal:
```bash
python quick_test.py
```

This will:
- ✓ Automatically test API connectivity
- ✓ Ingest 1,250 records (50 per batch × 25 batches)
- ✓ Display progress without prompts
- ✓ Complete in ~60 seconds

### Method 2: Standard Ingestion

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

## Usage

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

### Quick Testing Script

For rapid testing without prompts, use `quick_test.py`:

```python
# quick_test.py - Streamlined testing
from data_consumer import DataConsumer

# Configuration
API_URL = 'http://127.0.0.1:8000'
BATCH_SIZE = 50
TOTAL_BATCHES = 25

# Automatic connection test
consumer = DataConsumer(api_url=API_URL)
consumer.consume_continuous(
    batch_size=BATCH_SIZE,
    total_batches=TOTAL_BATCHES,
    delay=0.5
)
```

**Customize quick_test.py:**
- Edit `BATCH_SIZE` to change records per batch
- Edit `TOTAL_BATCHES` to change number of batches
- Edit `delay` to control throttling (seconds between batches)

---

##  How It Works

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

Else:
  MongoDB
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

##  Configuration

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


### Quick Test Output (quick_test.py)

```
======================================================================
QUICK TEST - AUTONOMOUS DATA INGESTION SYSTEM
======================================================================

Testing connection to http://127.0.0.1:8000...
✓ API server is reachable

Configuration:
  - Batch size: 50 records
  - Total batches: 25
  - Total records: 1,250
  - API URL: http://127.0.0.1:8000

Starting ingestion...

[Pipeline] Initialized successfully
[SQL] Added column: spo2 (INTEGER)
[SQL] Added column: purchase_value (REAL)
[Pipeline] Processed 50 records (SQL: 50, MongoDB: 50)
[Pipeline] Processed 100 records (SQL: 100, MongoDB: 100)
...
[Pipeline] Processed 1250 records (SQL: 1250, MongoDB: 1250)

=== Final Statistics ===
Total records processed: 1250
SQL inserts: 1250
MongoDB inserts: 1250
Errors: 0
Unique fields discovered: 58
Normalization rules: 12
Placement decisions made: 60

======================================================================
Test complete!
======================================================================
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

