# Section-by-Section Explanation (Normalization + PK/FK + Embed vs Reference)

This README explains the exact part of the report you shared, in simple language with examples.

## 1) Normalization Strategy (What it means)

Normalization means: do not keep repeating data inside one big root row. Split repeating data into child tables and connect them using a parent key.

In this project, three signals trigger normalization.

### 1.1 Arrays of objects

Meaning:
- A field contains a list of object records.
- Example: orders, comments, devices.

Input example:

```json
{
  "username": "u1",
  "orders": [
    {"order_id": "o1", "item": "book", "quantity": 1, "price": 10.0},
    {"order_id": "o2", "item": "bag", "quantity": 2, "price": 40.0}
  ]
}
```

How it is stored:
- Root table row in ingested_records
- Child rows in norm_orders
- Each child row has parent_sys_ingested_at linking back to root

Why:
- Keeps root table compact
- Supports one-to-many cleanly

### 1.2 Primitive arrays

Meaning:
- A field contains a list of scalar values, not objects.
- Example: tags, categories.

Input example:

```json
{
  "username": "u2",
  "tags": ["new", "sale", "popular"]
}
```

How it is stored:
- Child table: norm_tags
- Columns include value and item_index
- item_index preserves original order

Why:
- Avoids columns like tag1, tag2, tag3...
- Works for variable-length arrays

### 1.3 Repeated scalar groups

Meaning:
- Fields with a shared base name and numeric suffix.
- Example: phone1, phone2, phone3.

Input example:

```json
{
  "username": "u3",
  "phone1": "111-111",
  "phone2": "222-222",
  "phone3": "333-333"
}
```

How it is stored:
- Child table: norm_phone
- Values as rows with item_index
- Root table avoids extra phoneN columns

Why:
- Prevents root schema growth
- Makes repeated attributes queryable like a proper one-to-many list

## 2) Table Creation Logic (PK/FK/Indexes)

### 2.1 Root table

Table:
- ingested_records

Keys:
- id: primary key (auto increment)
- sys_ingested_at: unique join key

Meaning of sys_ingested_at:
- A per-record unique key used to link root and child data across SQL and Mongo flows.

### 2.2 Child tables

Typical child table columns:
- id (primary key)
- parent_sys_ingested_at (foreign key)
- item_index (order support)
- entity-specific scalar columns (for example order_id, item, quantity)

Foreign key rule:
- parent_sys_ingested_at references ingested_records(sys_ingested_at)

ON DELETE CASCADE means:
- If root row is deleted, child rows linked to that root are auto-deleted.

Performance index:
- Index on parent_sys_ingested_at speeds up fetching child rows for one parent.

## 3) MongoDB Design (Embed vs Reference)

The system computes a score S for each nested entity path.

Decision rule:
- reference if S >= 2
- embed if S < 2

Interpretation:
- One very strong signal or a few moderate signals push data to reference mode.
- Weak/small nested data usually stays embedded.

## 4) What Each Scoring Condition Means (with examples)

### 4.1 Array size

Condition:
- length > 10: +2
- length > 3: +1

Meaning:
- Bigger arrays are harder to keep inside one root document.

Example:
- comments length 12 -> +2
- comments length 5 -> +1
- comments length 2 -> +0

### 4.2 Array-of-objects

Condition:
- Array contains object elements: +1

Meaning:
- Arrays of objects are structurally heavier than arrays of scalars.

Example:
- devices: [{...}, {...}] -> +1
- tags: ["a", "b"] -> +0 for this criterion

### 4.3 Object width

Condition:
- object key count > 8: +2
- object key count > 5: +1

Meaning:
- Wider nested objects are more complex and likely to evolve.

Example:
- profile object with 9 keys -> +2
- settings object with 6 keys -> +1
- small object with 3 keys -> +0

### 4.4 Nesting depth

Condition:
- Path depth >= 3: +1

Meaning:
- Deeply nested fields are harder to maintain in one root document.

Example paths:
- preferences -> depth 1 -> +0
- user.profile -> depth 2 -> +0
- user.profile.devices -> depth 3 -> +1

### 4.5 Likely shared entity

Condition:
- Same terminal entity appears under multiple parent contexts: +1

Meaning:
- Data may represent reusable/shared structure; references are often safer.

Example:
- orders appears under customer.orders and supplier.orders -> +1

### 4.6 Schema hint: frequently_updated = true

Condition:
- frequently_updated true: +1

Meaning:
- If nested data changes often, referencing reduces large root rewrites.

Example:
- activity_log frequently updated -> +1

### 4.7 Schema hint: shared = true

Condition:
- shared true: +1

Meaning:
- Shared data patterns generally suit reference-style modeling.

Example:
- address/shared profile entity reused by multiple records -> +1

### 4.8 Schema hint: unbounded = true

Condition:
- unbounded true: +2

Meaning:
- Data can grow without clear limit; embedding may become too large.

Example:
- event history list expected to keep growing forever -> +2

### 4.9 Schema hint: expected_max_items > 10

Condition:
- expected_max_items greater than 10: +1

Meaning:
- Designer already expects a large list, so referencing is safer.

Example:
- expected_max_items = 100 for activity_log -> +1

## 5) Full Scoring Walkthrough Example

Entity path:
- activity_log

Value:
- Array length 12
- Array contains objects

Schema hints:
- frequently_updated = true
- unbounded = true

Score:
- array length > 10 -> +2
- array-of-objects -> +1
- frequently_updated -> +1
- unbounded -> +2

Total S = 6

Decision:
- S >= 2, so mode = reference

Storage behavior:
- Root document stores core fields
- activity_log items are stored in a reference collection, linked with parent_sys_ingested_at and entity_path

## 6) Why This Design Is Useful

- Normalization keeps SQL clean and scalable for repeating data.
- Embed/reference scoring keeps Mongo flexible without blindly embedding everything.
- Metadata stores decisions and reasons, so behavior is explainable and reproducible.
- Query engine can reconstruct merged JSON from SQL + Mongo using metadata mappings.

## 7) Quick Mental Model

Think of the system as three layers:

1. Analyzer:
- Learns field behavior and structure

2. Decision layer:
- Chooses SQL/Mongo/Buffer and embed/reference

3. Execution layer:
- Writes data and later rebuilds JSON for CRUD reads

That is exactly what your selected report section is describing.
