# Assignment 4: Dashboard Enhancement, Performance Evaluation & Final System Packaging

## 1. Project Objective

In the previous assignments, you have implemented the core components of a hybrid database framework including adaptive ingestion, metadata-driven storage decisions, automated query generation, and logical transaction coordination. This assignment focuses on completing the system by enhancing the dashboard capabilities, evaluating the performance of the hybrid framework, and packaging the entire system into a deployable and well-documented software framework. The objective is to demonstrate that the system can function as a complete logical database layer that provides a clean user interface while efficiently managing data across multiple storage backends.

## Core Technical Pipeline

The Assignment 4 implementation should extend the architecture developed in previous assignments with the following stages.

- **Phase 1: Dashboard Enhancement** Extend the logical dashboard to support improved navigation, entity inspection, and query monitoring.

- **Phase 2: Performance Benchmarking** Design experiments to measure system performance during data ingestion, query execution, and transaction coordination.

- **Phase 3: Comparative Evaluation** Compare the performance of logical queries executed through the framework with direct queries on SQL or MongoDB.

- **Phase 4: System Packaging** Prepare the final framework as a deployable software package with documentation and usage instructions.

---

## 2. Dashboard Enhancement Requirements

You must extend the dashboard implemented in Assignment 3 to provide a more comprehensive view of the logical database system. The dashboard should continue to present data according to the logical schema while hiding all backend implementation details.
The dashboard should support:

- Viewing active sessions
- Listing logical entities within a session
- Viewing instances of each entity
- Inspecting field names and values of logical objects
- Displaying results of executed logical queries
- Viewing query execution history

**Constraint:** The interface must not reveal backend-specific details such as SQL tables, MongoDB collections, indexing strategies, or schema placement decisions.

---

## 3. Performance Evaluation

You must design experiments to evaluate the performance of the hybrid database framework.
The performance analysis should consider:

- Data ingestion latency
- Logical query response time
- Metadata lookup overhead
- Transaction coordination overhead across SQL and MongoDB

The experiments should collect metrics such as:

- Average query latency
- Throughput (operations per second)
- Distribution of data across storage backends

Students should analyse how the abstraction layer affects system performance.

---

## 4. Comparative Analysis

You must compare the performance of the hybrid framework with direct database access. The goal of this comparison is to understand the trade-offs introduced by the logical abstraction layer.
Students should design experiments comparing:

- Retrieving user records through the logical query interface vs direct SQL queries
- Accessing nested documents using the framework vs direct MongoDB queries
- Updating records across multiple entities

The comparison should measure metrics such as:

- Query latency
- Update latency
- System throughput
- Query processing overhead introduced by the framework

Results should be presented using appropriate visualizations such as:

- Bar charts comparing query latency
- Line graphs showing throughput under increasing workload
- Tables summarizing performance metrics

Students should interpret the results and discuss scenarios where the logical abstraction introduces overhead as well as scenarios where it simplifies application development and improves data accessibility.

---

## 5. Final System Packaging

You must prepare the entire system as a complete and reproducible software package.
The final system should include:

- Source code repository (GitHub)
- Setup instructions for dependencies
- Instructions to configure SQL and MongoDB backends
- Instructions to run the ingestion API
- Instructions to run the logical query interface
- Instructions to launch the dashboard

The system should be organized so that another user can install and run the framework with minimal configuration effort.

---

## 6. Deliverables

The assignment submission must include:

- A single report: `group_name_final_report.pdf`
- A short demonstration video

**Report Requirements:**

- The first page must include:
  - GitHub repository link
  - Video demonstration link
- Description of dashboard enhancements
- Performance evaluation experiments
- Comparative analysis results
- Discussion of system limitations

---

## 7. Marking Criteria

| Criterion              | Focus Area                                          |
|------------------------|-----------------------------------------------------|
| Dashboard Enhancement  | Usability and logical data presentation             |
| Performance Evaluation | Quality of benchmarking experiments                 |
| Comparative Analysis   | Understanding of abstraction vs performance trade-offs |
| System Packaging       | Completeness and reproducibility of system setup    |
| Report Quality         | Technical clarity and explanation of experiments    |

---

## 8. Conclusion

This assignment completes the development of the hybrid database framework by evaluating its usability, performance, and deployability. The final system should demonstrate how logical abstraction and metadata-driven architectures can enable flexible and scalable data management across multiple database backends.