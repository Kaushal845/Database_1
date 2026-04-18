# Hybrid Database Framework Documentation

- Author: Shardul Junagade
- Course: Databases

This folder contains a complete, implementation-level documentation set for the Hybrid Database Framework built across Assignments 1 to 4.

The project goal is to provide one logical JSON CRUD interface while physically distributing data across SQL (SQLite) and MongoDB using runtime metadata, placement heuristics, normalization rules, and transaction coordination.


## Documentation modules

1. [01_Introduction_and_Architecture.md](01_Introduction_and_Architecture.md)
   System problem statement, architecture, component responsibilities, and data lifecycle.

2. [02_Adaptive_Ingestion_and_Placement.md](02_Adaptive_Ingestion_and_Placement.md)
   Assignment 1 implementation: semantic type detection, field statistics, placement decisions, and buffering/migration logic.

3. [03_Normalization_and_CRUD_Engine.md](03_Normalization_and_CRUD_Engine.md)
   Assignment 2 implementation: normalized SQL child tables, Mongo embed/reference strategy, metadata-driven CRUD planning and execution.

4. [04_Transactional_Validation_and_Logical_Dashboard.md](04_Transactional_Validation_and_Logical_Dashboard.md)
   Assignment 3 implementation: two-phase coordination model, rollback behavior, dashboard contracts, and logical abstraction constraints.

5. [05_Performance_and_Deployment.md](05_Performance_and_Deployment.md)
   Assignment 4 implementation: performance methodology, comparative results, overhead interpretation, and production-like Docker setup.

6. [06_Assignment_Requirements_Mapping.md](06_Assignment_Requirements_Mapping.md)
   Requirement-to-implementation matrix mapping each assignment expectation to concrete modules and behaviors.
