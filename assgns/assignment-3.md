# Assignment 3: Logical Dashboard & Transactional Validation

## 1. Project Objective

This assignment focuses on evaluating the usability and reliability of a hybrid database system through two key components:

- **Logical Dashboard Interface** – A user-facing interface that presents data in a clear, structured, and intuitive format. The dashboard should abstract away all underlying storage complexities and display information as cohesive logical entities, ensuring that users interact with data without needing to understand how or where it is stored.

- **Transactional Behaviour Validation** – A set of controlled experiments designed to verify that the system maintains ACID (Atomicity, Consistency, Isolation, Durability) properties during data operations, even when multiple storage backends are involved.

The goal of this assignment is to demonstrate that the system provides a stable and user-friendly interface while ensuring reliable and correct behaviour under transactional workloads.

## Core Technical Pipeline

The Assignment 3 implementation should extend the architecture developed in previous assignments with two additional layers.

- **Phase 1: Dashboard Rendering** Develop the dashboard to show the data from both the databases

- **Phase 2: Logical Data Reconstruction** Provide users with tools to query the data.

- **Phase 3: ACID Behaviour Validation** Design experiments that demonstrate atomicity, consistency, isolation, and durability within the hybrid storage system.

**Deadline**
6:00 PM, 5 April 2026

---

## 2. Logical Dashboard Requirements

This assignment requires the implementation of a logical dashboard that presents data in a user-centric form, independent of underlying storage mechanisms.
The dashboard should represent data strictly according to the logical schema and must not expose any physical storage details.
The dashboard must support:

- Viewing active session information
- Listing logical entities and their instances
- Displaying fields and corresponding values
- Presenting results of executed queries

**Constraint:** The interface must not reveal backend-specific details such as SQL table names, MongoDB collections, indexing, or schema decisions.
Any implementation medium (web-based or CLI) may be used. Evaluation will focus on correctness and clarity rather than UI complexity.

---

## 3. Logical Query Execution Monitoring

The dashboard must allow submission and observation of logical queries.
Example query:

```json
{
  "operation": "read",
  "fields": ["username", "comments"]
}
```

For each query, the system should:

- Interpret the request using metadata
- Generate and execute backend-specific queries
- Merge results into a unified logical response

The dashboard must display:

- Query input
- Logical result
- Execution status

---

## 4. Transaction Coordination Layer

This assignment requires a transaction coordination mechanism that ensures consistent execution across multiple backends.
The system should:

- Treat each operation as a single logical transaction
- Execute backend operations within their respective transactional guarantees
- Detect failures during execution
- Ensure all-or-nothing behaviour

On failure, all partial updates must be rolled back to prevent inconsistent state.

---

## 5. ACID Validation Experiments

The system must be evaluated through experiments validating the following properties:

### Atomicity

Operations must either fully complete or fully rollback across all backends.

### Consistency

All transactions must preserve defined constraints and valid system states.

### Isolation

Concurrent transactions must not interfere with each other or produce inconsistent results.

### Durability

Committed data must persist reliably, even after system interruptions.

---

## 6. Deliverables

The assignment submission must include:

- A single report: `group_name_report.pdf`
- A short demonstration video

**Report Requirements:**

- The first page must include:
  - GitHub repository link
  - Video demonstration link
- Description of dashboard design and implementation
- Explanation of transaction coordination mechanism
- ACID validation experiments and observations
- Discussion of system limitations

---

## 7. Marking Criteria

| Criterion                | Focus Area                        |
|--------------------------|-----------------------------------|
| Dashboard Implementation | Logical clarity and correctness   |
| Transaction Coordination | Reliable multi-backend execution  |
| ACID Validation          | Quality of experimental validation|
| System Robustness        | Failure and concurrency handling  |
| Report Quality           | Clarity and technical depth       |

---

## 8. Conclusion

This assignment evaluates the system's ability to provide a clear logical interface while maintaining ACID-compliant transactional behaviour across multiple storage backends.