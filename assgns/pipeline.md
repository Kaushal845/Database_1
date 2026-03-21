# Hybrid Ingestion & Storage Framework — Architecture Reference

## Overview

The system is a 9-step pipeline that accepts JSON input, routes data to SQL or MongoDB based on structure, and returns a unified JSON response.

---

## Pipeline Flow

### Step 1–5: Ingestion & Metadata Setup
1. **User** defines a JSON Schema
2. **JSON Schema Registry** stores the user-defined schema
3. **Data Ingestion API** receives incoming JSON data records
4. **Schema Interpretation** parses JSON fields and structure
5. **Metadata Manager** maintains:
   - JSON Schema
   - Field mappings
   - Storage location
   - Table / Collection mapping

### Step 6: Data Classification Engine
Decides where data should be stored:
- Structured → **SQL**
- Document / Nested → **MongoDB**
- Unknown → **Buffer**

Then routes via **Routing Logic (SQL / Mongo)** to one of three pipelines:

---

## Three Pipelines

### Pipeline 1 — Buffer
- Holds undecided/unclassified data temporarily

### Pipeline 2 — SQL Engine
- **SQL Normalization Unit**: Detects repeating groups, nested entities, relational dependencies
- **Table Creation Engine**: Creates normalized tables with PK/FK constraints
- Output: **SQL DB (Normalized Tables)**

### Pipeline 3 — MongoDB Engine
- **Document Structure Analyzer**: Decides whether to embed documents, create sub-collections, or reference documents
- **Collection Design Engine**: Creates collections, sub-collections, and document references
- Output: **MongoDB Collections**

---

## Step 7–9: Query & Response

7. **Query Generation Engine** uses metadata to generate:
   - SQL Queries
   - MongoDB Queries
   - Data Merge Logic

8. **CRUD Operations**:
   - `READ` → fetch & merge data
   - `INSERT` → store new records
   - `DELETE` → cascade/remove
   - `UPDATE` → delete + insert

9. **JSON Response** returned to user

---

## Key Design Rules for Implementation
- All classification decisions must be metadata-driven
- SQL and MongoDB pipelines operate in parallel after routing
- The Query Generation Engine must consult the Metadata Manager for every operation
- All final responses must be merged into a single JSON output regardless of which backend(s) were queried