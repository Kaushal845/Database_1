# AUTONOMOUS DATA INGESTION SYSTEM - TECHNICAL REPORT

**Generated:** 2026-02-13T20:24:26.320767

**System Status:** Active

**Total Records Processed:** 0

================================================================================

## 1. Normalization Strategy

### Problem Statement
Fields may appear with different casings (e.g., `ip`, `IP`, `IpAddress`) or naming
styles (`user_name` vs `userName`). Without normalization, these would create duplicate
columns in SQL, wasting storage and causing query complexity.

### Our Solution: Multi-Stage Normalization

#### Stage 1: Syntax Normalization
1. **CamelCase to snake_case conversion**
   - `userName` → `user_name`
   - `IpAddress` → `ip_address`
   - Uses regex pattern matching: `(.)([A-Z][a-z]+)` → `\1_\2`

2. **Lowercase conversion**
   - All field names converted to lowercase
   - `IP` → `ip`, `Email` → `email`

3. **Underscore cleanup**
   - Multiple underscores collapsed: `user__name` → `user_name`
   - Leading/trailing underscores removed

#### Stage 2: Semantic Normalization
Applied pattern-based rules to map semantically equivalent fields:

```
IP Patterns:     ip, IP, ipAddress, ip_address → ip_address
User Patterns:   userName, user_name, username → username
Email Patterns:  email, eMail, emailAddress → email
Time Patterns:   timestamp, timeStamp, t_stamp → timestamp
GPS Patterns:    lat, latitude, gps_lat → gps_lat
```

#### Implementation Details
- **Module:** `field_normalizer.py`
- **Method:** `FieldNormalizer.normalize(field_name)`
- **Storage:** Normalization mappings persisted in `metadata_store.json`
  under `normalization_rules` for consistency across restarts

### Observed Normalizations

*No ambiguous field names encountered yet.*

### Rules Enforcement
1. **First occurrence wins**: When a field is first seen, its normalized form is recorded
2. **Consistent mapping**: All future occurrences of variants map to the same normalized key
3. **Persistent storage**: Mappings survive system restarts via metadata store
4. **Case-insensitive matching**: `IP` and `ip` are treated identically


## 2. Placement Heuristics: SQL vs MongoDB

### Decision Framework
Our system uses a **multi-criteria decision engine** with adaptive thresholds:

### Thresholds Configuration
- **Frequency Threshold:** 60.0%
- **Type Stability Threshold:** 80.0%
- **Minimum Observations:** 10 records

### Placement Rules (in priority order)

#### Rule 1: Mandatory Fields → BOTH
Fields: `username`, `sys_ingested_at`, `t_stamp`
- **Rationale:** These fields enable joining data across SQL and MongoDB
- **Use case:** Query SQL for structured data, then fetch full document from MongoDB
  using `sys_ingested_at` as the join key

#### Rule 2: Nested/Array Fields → MongoDB
Condition: `type == 'dict' OR type == 'list'`
- **Rationale:** SQL requires flattening or JSON serialization (lossy)
- **MongoDB advantage:** Native support for nested documents and arrays
- **Example:** `metadata: {sensor: {version: '2.1'}}`

#### Rule 3: High Frequency + Stable Type → SQL
Conditions:
- Frequency ≥ 60.0%
- Type Stability ≥ 80.0%
- **Rationale:** Consistent, frequently-appearing fields benefit from SQL's schema
- **SQL advantages:**
  - Efficient storage (fixed schema)
  - Fast queries with indexes
  - Type constraints prevent errors
- **Example:** `email` appears in 95% of records, always as string

#### Rule 4: Low Frequency → MongoDB
Condition: Frequency < 60.0%
- **Rationale:** Sparse fields waste space in SQL (many NULL values)
- **MongoDB advantage:** Fields only consume space when present
- **Example:** `altitude` appears in only 30% of records

#### Rule 5: Type Drifting → MongoDB
Condition: Type Stability < 80.0%
- **Rationale:** SQL schemas expect consistent types
- **MongoDB advantage:** Flexible schema handles type variations
- **Example:** `battery` sometimes integer (50), sometimes string ('50%')

### Placement Distribution

- **SQL Only:** 0 fields
- **MongoDB Only:** 0 fields
- **Both:** 0 fields

### Example Placements


## 3. Uniqueness Detection Strategy

### Problem Statement
Not all frequent fields should be UNIQUE. For example, `username` is frequent but
non-unique (from a pool of 1000 users). We need intelligent heuristics to identify
true unique identifiers.

### Our Multi-Factor Approach

#### Factor 1: Name-Based Heuristics
Field names containing these keywords suggest uniqueness:
- `id` (e.g., `device_id`, `session_id`, `user_id`)
- `uuid`
- `key`
- `session`

**Rationale:** Naming conventions often signal intent

#### Factor 2: Type-Based Heuristics
Fields with these semantic types suggest uniqueness:
- `uuid` (e.g., '550e8400-e29b-41d4-a716-446655440000')
- `integer` (when combined with name heuristic)

**Rationale:** UUIDs are designed for uniqueness; integer IDs are common patterns

#### Factor 3: Cardinality Analysis
We track sample values and compute:
```
Unique Ratio = Number of Unique Values / Total Values
```
- If Unique Ratio > 0.9 (90%), cardinality is high

**Rationale:** True unique fields have high diversity

### Decision Logic
A field is marked UNIQUE if:
```
Has Name Indicator (id/uuid/key)
  AND
(Has Unique Type OR High Cardinality)
```

### Special Cases
- **`username`**: Explicitly excluded (non-unique by design - 1000-user pool)
- **`sys_ingested_at`**: Marked UNIQUE (server-generated with microsecond precision)
- **Frequent non-IDs**: `email`, `phone` are frequent but not enforced as UNIQUE
  (may have duplicates in real-world data)

### Implementation
- **Module:** `placement_heuristics.py`
- **Method:** `PlacementHeuristics.should_be_unique(field_name)`
- **Application:** SQL schema uses `CREATE UNIQUE INDEX` for identified fields


## 4. Value Interpretation: Semantic Type Detection

### Challenge
Values can be ambiguous:
- `'1.2.3.4'` could be a string or an IP address
- `1.2` is clearly a float
- `'1.2'` is a string representing a number

Naive `type()` checking only reveals Python types, not semantic meaning.

### Our Solution: Cascading Type Detection
**Module:** `type_detector.py`

#### Detection Priority (checked in order)

**1. Null Check**
   - `value is None` → `'null'`

**2. Boolean Check**
   - `isinstance(value, bool)` → `'boolean'`
   - Must come before int check (bool is subclass of int in Python)

**3. Integer Check**
   - `isinstance(value, int)` → `'integer'`

**4. Float Check**
   - `isinstance(value, float)` → `'float'`
   - Example: `1.2` → `'float'`

**5. Collection Checks**
   - `isinstance(value, list)` → `'list'`
   - `isinstance(value, dict)` → `'dict'`

**6. String Semantic Analysis** (most complex)
   For string values, we apply regex patterns:

   **a) UUID Pattern**
   ```regex
   ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$
   ```
   Example: `'550e8400-e29b-41d4-a716-446655440000'` → `'uuid'`

   **b) IP Address Pattern** (CRITICAL for question 4)
   ```regex
   ^(\d{1,3}\.){3}\d{1,3}$
   ```
   - Matches 4 dot-separated numeric parts
   - Additional validation: Each part must be 0-255
   - Example: `'1.2.3.4'` → `'ip_address'` (validated ✓)
   - Example: `'1.2'` → `'string'` (only 2 parts, fails pattern)
   - Example: `'999.1.1.1'` → `'string'` (999 > 255, invalid)

   **c) Email Pattern**
   ```regex
   ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
   ```
   Example: `'user@example.com'` → `'email'`

   **d) URL Pattern**
   ```regex
   ^https?://[^\s]+$
   ```
   Example: `'https://example.com'` → `'url'`

   **e) ISO Timestamp Pattern**
   ```regex
   ^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}
   ```
   Example: `'2024-01-15T10:30:00'` → `'timestamp'`

   **f) Fallback**
   - If no pattern matches → `'string'`

### Key Insight: Order Matters
IP detection happens **before** generic string classification, ensuring:
- `'1.2.3.4'` is recognized as `ip_address`, not `string`
- `1.2` is already caught by float check, never reaches string analysis

### Type Mapping to SQL
Once semantic type is detected, we map to SQL types:
```
boolean      → BOOLEAN
integer      → INTEGER
float        → REAL
ip_address   → VARCHAR(15)
uuid         → VARCHAR(36)
email        → VARCHAR(255)
url          → TEXT
timestamp    → TIMESTAMP
string       → TEXT
```

### Verification Example
```python
TypeDetector.detect_type('1.2.3.4')   # → 'ip_address'
TypeDetector.detect_type(1.2)         # → 'float'
TypeDetector.detect_type('1.2')       # → 'string' (only 2 parts)
TypeDetector.detect_type('192.168.1.1')  # → 'ip_address'
```


## 5. Mixed Data Handling: Type Drifting

### Problem Scenario
A field like `battery` might arrive as:
- Record 1: `50` (integer)
- Record 2: `'60%'` (string)
- Record 3: `75.5` (float)
- Record 4: `'charging'` (string)

SQL expects consistent types, but real-world data is messy.

### Our Adaptive Strategy

#### Phase 1: Detection & Tracking
For every field in every record:
1. Detect semantic type using `TypeDetector`
2. Increment type counter in metadata:
   ```python
   field_data['type_counts'][detected_type] += 1
   ```
3. Calculate type stability:
   ```python
   stability = (dominant_type_count / total_appearances) * 100
   ```

#### Phase 2: Dynamic Placement Adjustment
- If initial observations suggest SQL (high frequency, stable type)
  → Field starts going to SQL
- If type stability drops below 80.0%
  → **Placement decision updated to MongoDB**
  → Reason logged: 'Type drifting detected'

#### Phase 3: Coexistence Strategy
What happens to data already in SQL?
- **SQL Handling:**
  - Column type set to `TEXT` (most flexible)
  - All values stored as strings: `50`, `'60%'`, `'charging'`
  - No data loss, but type constraints relaxed
- **MongoDB Handling:**
  - New records with this field go to MongoDB
  - Native type preservation: integer stays integer, string stays string
  - No conversion needed

#### Phase 4: Metadata Persistence
All type tracking survives restarts:
```json
{
  'fields': {
    'battery': {
      'appearances': 100,
      'type_counts': {
        'integer': 50,
        'string': 40,
        'float': 10
      },
      'type_stability': 50.0,  // 50% integer
      'placement': 'MongoDB'  // Updated due to instability
    }
  }
}
```

### Real-World Impact

*No type drifting detected in current dataset.*

### Advantages of Our Approach
1. **No data loss**: All records stored regardless of type inconsistency
2. **Graceful degradation**: SQL fields degrade to TEXT when needed
3. **Automatic adaptation**: System learns and adjusts without manual intervention
4. **Audit trail**: All type changes logged in metadata with timestamps
5. **Restart resilience**: Type history preserved across system restarts


## 6. System Statistics

- **Total Records Ingested:** 0
- **Unique Fields Discovered:** 0
- **Normalization Rules Created:** 0
- **Placement Decisions Made:** 0
- **Session Start:** 2026-02-13T20:24:26.320767
- **Last Updated:** 2026-02-13T20:24:26.320767


## 7. Field-by-Field Analysis

| Field Name | Frequency | Type | Stability | Placement | Unique |
|------------|-----------|------|-----------|-----------|--------|


## 8. System Architecture

### Component Overview

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
│  ┌──────────────────────────────────────────────────┐  │
│  │ 1. Field Normalizer (field_normalizer.py)       │  │
│  │    - Resolve naming ambiguities                  │  │
│  └──────────────────────────────────────────────────┘  │
│                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │ 2. Type Detector (type_detector.py)              │  │
│  │    - Semantic type detection                     │  │
│  └──────────────────────────────────────────────────┘  │
│                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │ 3. Metadata Store (metadata_store.py)            │  │
│  │    - Track frequency & type stability            │  │
│  └──────────────────────────────────────────────────┘  │
│                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │ 4. Placement Heuristics (placement_heuristics.py)│  │
│  │    - Decide SQL vs MongoDB                       │  │
│  └──────────────────────────────────────────────────┘  │
│                     │
└─────────────────────┼───────────────────────────────────┘
                      │
        ┌─────────────┴──────────────┐
        ▼                            ▼
 ┌─────────────┐            ┌──────────────┐
 │ SQL Manager │            │ MongoDB Mgr  │
 │  (SQLite)   │            │   (MongoDB)  │
 └─────────────┘            └──────────────┘
```

### Key Files
- `metadata_store.py` - Persistent metadata storage
- `field_normalizer.py` - Field name normalization
- `type_detector.py` - Semantic type detection
- `placement_heuristics.py` - Placement decision logic
- `database_managers.py` - SQL & MongoDB interfaces
- `ingestion_pipeline.py` - Main orchestration
- `data_consumer.py` - API client
- `report_generator.py` - This report


## 9. Conclusion

### Key Achievements
1. **Autonomous Operation**: No hardcoded field mappings
2. **Adaptive Learning**: System improves decisions as more data arrives
3. **Persistent Memory**: Survives restarts via metadata store
4. **Data Integrity**: Bi-temporal timestamps enable accurate joins
5. **Flexibility**: Handles unclean, heterogeneous data gracefully

### Design Principles
- **No Hardcoding**: All decisions data-driven
- **Gradual Learning**: Accumulate knowledge over time
- **Fail-Safe Defaults**: Unknown fields → MongoDB (flexible)
- **Traceability**: Username + sys_ingested_at maintained everywhere

### Future Enhancements
- Machine learning for placement optimization
- Automatic index creation based on query patterns
- Distributed processing for high-volume streams
- Real-time monitoring dashboard

---

*Report generated by `report_generator.py` on 2026-02-13T20:24:26.321350*
