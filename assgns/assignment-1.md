# Assignment 1: Adaptive Ingestion & Hybrid Backend Placement

## 1. Project Objective

The primary goal of this assignment is to develop an autonomous data ingestion system that dynamically determines the optimal storage backend—SQL or MongoDB—for incoming JSON records. Unlike traditional systems where schemas are predefined, this program must observe data patterns, analyze field behaviors, and make data-driven placement decisions without human intervention.

## Core Technical Pipeline:

- **Phase 1: Ingestion & Normalization**
  Consuming live JSON streams and resolving naming ambiguities.

- **Phase 2: Data Analysis**
  Evaluating frequency, type stability, and structural complexity of each field.

- **Phase 3: Heuristic Classification**
  Applying autonomous logic to assign fields to SQL or MongoDB.

- **Phase 4: Commit & Routing**
  Migrating buffered data and establishing persistent metadata routing.

**Deadline**
15 February, 2026

---

## 2. The Data Stream (API Specification)

You can use the following github repository to get the data stream as JSON for your implementation. The steps to use the code are in the README file in the same folder. Students must expect "unclean" and heterogeneous records.

### Data Characteristics

- **Key Ambiguity:** Fields may appear with different casings (e.g., `ip`, `IP`, `IpAddress`) or naming styles (`user_name` vs `userName`).

- **Type Drifting:** A field may arrive as an integer in one record and a string in the next.

- **Nesting:** High-value data may be buried in nested dictionaries or arrays.

- **Traceability:** Every record will contain a `username`. Even if data is split across backends, this field must be preserved in both to maintain a link.

---

## 3. System Logic & Constraints

To ensure the system is truly adaptive and reliable, the following rules are enforced:

- **Bi-Temporal Timestamps:**
  1. **Client Timestamp (`t_stamp`):** Use the timestamp provided in the JSON for historical context.
  2. **Server Timestamp (`sys_ingested_at`): Autogenerate** a unique timestamp on the server side upon receipt. This must be stored in **both** SQL and MongoDB for a single record to act as a join key.

- **No Hardcoding:** Field mappings must be discovered dynamically.

- **Persistence:** Classification decisions must be stored in a metadata store to survive restarts.

---

## 4. Mandatory Report Questions

Upon completion, teams must submit a technical report answering the following:

1. **Normalization Strategy:** How did you resolve naming ambiguities (e.g., `ip` vs `IP`)? What rules did you follow to ensure they didn't create duplicate columns?

2. **Placement Heuristics:** What specific thresholds (e.g., frequency %) were used to decide between SQL and MongoDB?

3. **Uniqueness:** How did you identify which fields should be marked as `UNIQUE` in SQL versus those that are just frequent?

4. **Value Interpretation:** How did your system differentiate between a string representing an IP (`"1.2.3.4"`) and a float (`1.2`)?

5. **Mixed Data Handling:** How did your system react when a field's data type changed mid-stream?

---

## 5. Marking Criteria

| Criterion              | Focus Area                                              |
|------------------------|---------------------------------------------------------|
| Data Tracking          | Accuracy of frequency and type stability metrics.       |
| Classification Quality | Justification of placement logic and normalization.     |
| System Integrity       | Proper handling of dual timestamps and global `username`. |
| Metadata Persistence   | Ability to remember decisions across restarts.          |

---

## 6. Conclusion

Assignment 1 establishes the foundation for a hybrid database engine. By decoupling the ingestion from the storage decision, the system achieves a "schema-on-read" flexibility while maintaining the performance benefits of relational storage for structured data.