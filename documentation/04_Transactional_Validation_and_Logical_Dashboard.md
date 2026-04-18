Back to [home](README.md)

# 4. Transactional Validation and Logical Dashboard (Assignment 3)

## 4.1 Assignment objective

Assignment 3 adds two critical system qualities:

- Cross-backend write reliability (ACID-oriented behavior across SQL + MongoDB).
- A logical dashboard that exposes data semantics without exposing physical storage internals.

## 4.2 Transaction coordinator design

`transaction_coordinator.py` introduces a coordinated transaction lifecycle:

- `PENDING`
- `PREPARED`
- `COMMITTED`
- `ABORTED`
- `FAILED`

Core objects:

- `Transaction`: transaction context and operation queue.
- `TransactionOperation`: one planned insert/update/delete operation.

The coordinator tracks active transactions with thread-safe locking.

## 4.3 Two-phase-like write flow

### Begin

- Starts SQLite `BEGIN IMMEDIATE` and savepoint.
- Registers transaction metadata in Mongo `_transactions`.

### Prepare

- SQL side: stage operation, capture rollback data, run SQL prepare execution.
- Mongo side: stage operation in `_transaction_temp` rather than final collection.

### Commit

- Release SQL savepoint and commit SQL transaction.
- Replay staged Mongo temp operations to main collection.
- Clean temp docs and transaction metadata.

### Abort

- Roll back SQL to savepoint.
- Remove staged Mongo temp entries.
- Mark transaction as aborted and clear coordinator state.

This gives atomic behavior at application level across two independent persistence engines.

## 4.4 Query engine integration with transactions

`query_engine.py` integrates coordinator for write operations:

- Reads bypass transaction wrapper.
- Inserts use ingestion callback path.
- Updates/deletes are enqueued in transaction operations, then prepared and committed.

If prepare or commit fails, abort path is invoked and an error response is returned.

## 4.5 ACID behavior in this implementation

- Atomicity: coordinated commit or coordinated abort on failure.
- Consistency: metadata mapping validation and controlled write routing.
- Isolation: SQLite transaction boundary with immediate lock for write intent.
- Durability: committed SQL + finalized Mongo writes persist; metadata persisted to file.

Practical note: this is an application-coordinated 2PC-style approach, not native distributed XA across engines.

## 4.6 Dashboard backend contract

`dashboard_api.py` serves two purposes in one process:

- API endpoints.
- Built dashboard static assets.

Key dashboard-facing endpoints include:

- `/api/dashboard/summary`
- `/api/dashboard/records`
- `/api/dashboard/session`
- `/api/dashboard/entities`
- `/api/dashboard/field-placements`
- `/api/dashboard/query-history`

Operational endpoints:

- `/health`
- `/api/query`
- transaction status endpoints

Compatibility endpoints for existing scripts:

- `/schema`
- `/record/{count}` (SSE)

## 4.7 Logical UI model

React dashboard components are organized by logical concerns:

- Session visibility
- Entity catalog
- Record browser
- Field placement summaries
- Query builder and query history

The UI uses metadata and merged record responses, not direct SQL schema browsing. This preserves the assignment constraint that users interact with logical entities rather than backend storage internals.

## 4.8 Known operational constraints

From implementation and reports:

- Query history is in-memory and cleared on API restart.
- SQLite write locking limits high-concurrency write throughput.
- Cross-entity relational joins are not a first-class user-facing logical operation yet.

Even with these constraints, Assignment 3 delivers a coherent logical interface backed by coordinated cross-backend writes.