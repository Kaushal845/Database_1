# Usage Guide - Autonomous Data Ingestion System

Complete step-by-step guide to using the autonomous data ingestion system.

---

## Table of Contents

1. [Quick Start (Recommended)](#quick-start-recommended)
2. [Manual Usage](#manual-usage)
3. [Advanced Usage](#advanced-usage)
4. [Querying the Databases](#querying-the-databases)
5. [Understanding the Output](#understanding-the-output)
6. [Common Scenarios](#common-scenarios)

---

## Quick Start (Recommended)

### Option 1: Using the Quick Start Script

```bash
# Terminal 1: Start the data generator
uvicorn main:app --reload --port 8000

# Terminal 2: Run the quick start script
python quickstart.py
```

The script will:
- Check dependencies
- Detect MongoDB availability
- Guide you through configuration
- Run the ingestion
- Generate the report automatically

---

## Manual Usage

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 2: Start the Data Generator

Open Terminal 1:

```bash
uvicorn main:app --reload --port 8000
```

Expected output:
```
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [xxxxx] using StatReload
INFO:     Started server process [xxxxx]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### Step 3: Test the API

Open your browser or use curl:

```bash
# Browser
http://127.0.0.1:8000/

# or using curl
curl http://127.0.0.1:8000/
```

You should see a JSON record.

### Step 4: Run the Ingestion System

Open Terminal 2:

```bash
# Default: 100 records per batch, 10 batches (1,000 total)
python data_consumer.py

# Custom: 50 records per batch, 20 batches (1,000 total)
python data_consumer.py 50 20

# Large test: 100 records per batch, 100 batches (10,000 total)
python data_consumer.py 100 100
```

### Step 5: Monitor Progress

You'll see output like:

```
====================================================================
AUTONOMOUS DATA INGESTION SYSTEM
====================================================================

[Pipeline] Initialized successfully
[Pipeline] Loaded 0 known fields
[Consumer] Testing connection to http://127.0.0.1:8000...
[Consumer] ✓ API server is reachable

[Consumer] === Batch 1/10 ===
[SQL] Added column: email (VARCHAR(255))
[SQL] Added column: age (INTEGER)
[SQL] Added column: device_id (VARCHAR(36))
[Pipeline] Processed 50 records (SQL: 50, MongoDB: 50)
[Pipeline] Processed 100 records (SQL: 100, MongoDB: 100)

[Consumer] === Batch 2/10 ===
...
```

### Step 6: Generate the Report

After ingestion completes:

```bash
python report_generator.py
```

Output:
```
[Report] Loading metadata from: metadata_store.json
[Report] ✓ Report generated successfully!
[Report] File: TECHNICAL_REPORT.md
[Report] Size: XXXXX characters
```

### Step 7: Review Results

1. **Read the report:**
   ```bash
   # Windows
   notepad TECHNICAL_REPORT.md
   
   # Linux/Mac
   cat TECHNICAL_REPORT.md
   # or
   less TECHNICAL_REPORT.md
   ```

2. **Check metadata:**
   ```bash
   # Windows
   notepad metadata_store.json
   
   # Linux/Mac
   cat metadata_store.json
   ```

---

## Advanced Usage

### Custom Pipeline Configuration

```python
from ingestion_pipeline import IngestionPipeline

# Create pipeline with custom settings
pipeline = IngestionPipeline(
    metadata_file='my_metadata.json',
    sql_db='my_database.db',
    mongo_uri='mongodb://localhost:27017/',
    mongo_db='my_mongo_db'
)

# Ingest data from your source
import json

with open('my_data.json', 'r') as f:
    for line in f:
        record = json.loads(line)
        pipeline.ingest_record(record)

# Get statistics
stats = pipeline.get_statistics()
print(f"Processed: {stats['pipeline']['total_processed']} records")
print(f"SQL fields: {stats['placement']['sql_count']}")
print(f"MongoDB fields: {stats['placement']['mongodb_count']}")

# Save and close
pipeline.close()
```

### Adjusting Placement Thresholds

Edit `placement_heuristics.py`:

```python
class PlacementHeuristics:
    # Increase frequency threshold (more fields to MongoDB)
    FREQUENCY_THRESHOLD = 70.0  # Was: 60.0
    
    # Increase stability threshold (more fields to MongoDB)
    TYPE_STABILITY_THRESHOLD = 90.0  # Was: 80.0
    
    # Require more observations before deciding
    MIN_OBSERVATIONS = 50  # Was: 10
```

### Custom Field Normalization Rules

Edit `field_normalizer.py`:

```python
class FieldNormalizer:
    def __init__(self):
        # Add your custom patterns
        self.semantic_patterns = {
            # Existing patterns...
            
            # Add custom patterns
            r'^custom_field$': 'my_normalized_name',
            r'^(old|legacy)_name$': 'new_name',
        }
```

### Using Different Data Sources

#### From CSV

```python
import csv
from ingestion_pipeline import IngestionPipeline

pipeline = IngestionPipeline()

with open('data.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        pipeline.ingest_record(dict(row))

pipeline.close()
```

#### From API

```python
import requests
from ingestion_pipeline import IngestionPipeline

pipeline = IngestionPipeline()

response = requests.get('https://api.example.com/data')
for record in response.json():
    pipeline.ingest_record(record)

pipeline.close()
```

#### From Kafka/Stream

```python
from kafka import KafkaConsumer
from ingestion_pipeline import IngestionPipeline
import json

pipeline = IngestionPipeline()
consumer = KafkaConsumer('my-topic', bootstrap_servers=['localhost:9092'])

for message in consumer:
    record = json.loads(message.value)
    pipeline.ingest_record(record)

pipeline.close()
```

---

## Querying the Databases

### SQLite Queries

#### Using Command Line

```bash
# Windows
sqlite3 ingestion_data.db

# Linux/Mac
sqlite3 ingestion_data.db
```

#### Example Queries

```sql
-- View schema
.schema ingested_records

-- Count records
SELECT COUNT(*) FROM ingested_records;

-- View first 10 records
SELECT * FROM ingested_records LIMIT 10;

-- Find records by username
SELECT * FROM ingested_records WHERE username = 'user123';

-- Group by email domain
SELECT 
    SUBSTR(email, INSTR(email, '@') + 1) as domain,
    COUNT(*) as count
FROM ingested_records
WHERE email IS NOT NULL
GROUP BY domain
ORDER BY count DESC;

-- Find records with specific timestamp
SELECT * FROM ingested_records 
WHERE sys_ingested_at LIKE '2024-02-14%';
```

#### Using Python

```python
import sqlite3

conn = sqlite3.connect('ingestion_data.db')
cursor = conn.cursor()

# Get all usernames
cursor.execute("SELECT DISTINCT username FROM ingested_records")
usernames = cursor.fetchall()
print(f"Unique users: {len(usernames)}")

# Get record count
cursor.execute("SELECT COUNT(*) FROM ingested_records")
count = cursor.fetchone()[0]
print(f"Total records: {count}")

conn.close()
```

### MongoDB Queries

#### Using Command Line

```bash
mongo ingestion_db
```

#### Example Queries

```javascript
// Count documents
db.ingested_records.count()

// Find all records for a user
db.ingested_records.find({username: "user123"})

// Find records with nested metadata
db.ingested_records.find({"metadata": {$exists: true}}).pretty()

// Find records with specific nested value
db.ingested_records.find({"metadata.sensor_data.version": "2.1"})

// Aggregation: Count by field existence
db.ingested_records.aggregate([
    {
        $project: {
            hasMetadata: {$cond: [{$ifNull: ["$metadata", false]}, 1, 0]},
            hasAltitude: {$cond: [{$ifNull: ["$altitude", false]}, 1, 0]}
        }
    },
    {
        $group: {
            _id: null,
            withMetadata: {$sum: "$hasMetadata"},
            withAltitude: {$sum: "$hasAltitude"}
        }
    }
])
```

#### Using Python

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['ingestion_db']
collection = db['ingested_records']

# Count documents
count = collection.count_documents({})
print(f"Total documents: {count}")

# Find documents with nested data
nested_docs = collection.find({"metadata": {"$exists": True}})
print(f"Documents with metadata: {nested_docs.count()}")

# Get unique usernames
usernames = collection.distinct("username")
print(f"Unique users: {len(usernames)}")

client.close()
```

### Joining SQL and MongoDB

```python
import sqlite3
from pymongo import MongoClient

# Connect to both databases
sql_conn = sqlite3.connect('ingestion_data.db')
sql_cursor = sql_conn.cursor()

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['ingestion_db']
mongo_collection = mongo_db['ingested_records']

# Query SQL for structured data
sql_cursor.execute("""
    SELECT sys_ingested_at, username, email 
    FROM ingested_records 
    WHERE username = 'user123'
""")

# For each SQL result, fetch full document from MongoDB
for row in sql_cursor.fetchall():
    sys_time, username, email = row
    
    # Join using sys_ingested_at
    mongo_doc = mongo_collection.find_one({"sys_ingested_at": sys_time})
    
    print(f"SQL: {email}")
    print(f"MongoDB: {mongo_doc.get('metadata', 'No metadata')}")

sql_conn.close()
mongo_client.close()
```

---

## Understanding the Output

### Console Output Explained

```
[Pipeline] Initialized successfully
```
✓ System started, components loaded

```
[Pipeline] Loaded 0 known fields
```
✓ No previous metadata (fresh start) or number of known fields

```
[Consumer] ✓ API server is reachable
```
✓ Successfully connected to data generator

```
[SQL] Added column: email (VARCHAR(255))
```
✓ New field discovered, added to SQL schema dynamically

```
[Pipeline] Processed 50 records (SQL: 50, MongoDB: 50)
```
✓ Progress update (every 50 records)

```
[MongoDB] Connection failed: ...
```
⚠️ MongoDB unavailable - system continues with SQL only

### Generated Files

#### metadata_store.json

Contains:
- All field statistics
- Normalization rules
- Placement decisions
- Type tracking

```json
{
  "fields": {
    "email": {
      "appearances": 950,
      "type_counts": {"email": 950},
      "first_seen": "2024-02-14T10:30:00",
      "sample_values": ["user@test.com", ...]
    }
  },
  "normalization_rules": {
    "Email": "email",
    "emailAddress": "email"
  },
  "placement_decisions": {
    "email": {
      "backend": "SQL",
      "reason": "High frequency (95%) and stable type"
    }
  },
  "total_records": 1000
}
```

#### ingestion_data.db

SQLite database with:
- Table: `ingested_records`
- Dynamically created columns
- All SQL-compatible fields

#### TECHNICAL_REPORT.md

Comprehensive report answering:
1. Normalization strategy
2. Placement heuristics
3. Uniqueness detection
4. Value interpretation
5. Type drifting handling

---

## Common Scenarios

### Scenario 1: Fresh Start

```bash
# Remove old data
rm metadata_store.json
rm ingestion_data.db

# Start fresh
python data_consumer.py 100 10
```

### Scenario 2: Continue Previous Session

```bash
# Keep metadata_store.json
# System will load previous decisions

python data_consumer.py 100 10

# New data uses existing rules
# Metadata accumulates
```

### Scenario 3: Large Dataset

```bash
# Ingest 100,000 records
python data_consumer.py 1000 100

# This will take ~5-10 minutes
```

### Scenario 4: SQL Only (No MongoDB)

```bash
# System automatically detects MongoDB unavailability
# All data goes to SQL (nested data serialized as JSON strings)

python data_consumer.py
```

### Scenario 5: Custom Data Source

```python
# Create a custom ingestion script
from ingestion_pipeline import IngestionPipeline
import json

pipeline = IngestionPipeline()

# Your custom data source
with open('my_data.jsonl', 'r') as f:
    for line in f:
        record = json.loads(line)
        # Add required field if missing
        if 'username' not in record:
            record['username'] = 'unknown'
        pipeline.ingest_record(record)

pipeline.close()

# Generate report
from report_generator import ReportGenerator
from metadata_store import MetadataStore

store = MetadataStore('metadata_store.json')
generator = ReportGenerator(store)
generator.generate_full_report()
```

### Scenario 6: Testing Normalization

```python
from field_normalizer import FieldNormalizer

normalizer = FieldNormalizer()

test_cases = [
    "userName", "user_name", "UserName", "username",
    "emailAddress", "email", "Email", "eMail",
    "IP", "ip", "IpAddress", "ip_address"
]

for field in test_cases:
    normalized = normalizer.normalize(field)
    print(f"{field:20} → {normalized}")
```

### Scenario 7: Testing Type Detection

```python
from type_detector import TypeDetector

detector = TypeDetector()

test_values = [
    "192.168.1.1",
    1.2,
    "1.2",
    "user@example.com",
    "550e8400-e29b-41d4-a716-446655440000",
    [1, 2, 3],
    {"nested": "value"}
]

for value in test_values:
    detected = detector.detect_type(value)
    print(f"{str(value):40} → {detected}")
```

---

## Troubleshooting Commands

### Check if API is running

```bash
# Windows
netstat -an | findstr 8000

# Linux/Mac
lsof -i :8000
```

### Check MongoDB status

```bash
# Windows
tasklist | findstr mongod

# Linux/Mac
ps aux | grep mongod
```

### View database file size

```bash
# Windows
dir ingestion_data.db

# Linux/Mac
ls -lh ingestion_data.db
```

### Count records in databases

```bash
# SQLite
sqlite3 ingestion_data.db "SELECT COUNT(*) FROM ingested_records;"

# MongoDB
mongo ingestion_db --eval "db.ingested_records.count()"
```

---

## Next Steps

After successful ingestion:

1. ✅ Read `TECHNICAL_REPORT.md` for analysis
2. ✅ Explore `metadata_store.json` to understand decisions
3. ✅ Query the databases using examples above
4. ✅ Experiment with different thresholds
5. ✅ Try your own data sources

---

**Happy Ingesting! 🚀**

For more information, see [README_SYSTEM.md](README_SYSTEM.md)
