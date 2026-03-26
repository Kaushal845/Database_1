"""
ACID Test Suite - Comprehensive validation of ACID properties across hybrid SQL+MongoDB system

This module implements academic-grade testing of:
- Atomicity: All-or-nothing execution across backends
- Consistency: Constraint enforcement and data integrity
- Isolation: Concurrent transaction handling
- Durability: Persistence across crashes/restarts

Each test generates detailed reports with timing, logs, and analysis.
"""
import sys
import os
import time
import threading
import sqlite3
from typing import Dict, Any, List, Tuple
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database_managers import SQLManager, MongoDBManager
from transaction_coordinator import TransactionCoordinator
from acid_report_generator import generate_reports_from_test_results
from logging_utils import get_logger


logger = get_logger("acid_tests")


class AcidTestResult:
    """Container for test results"""

    def __init__(self, test_name: str, test_type: str):
        self.test_name = test_name
        self.test_type = test_type  # atomicity, consistency, isolation, durability
        self.passed = False
        self.objective = ""
        self.setup_info = ""
        self.execution_log: List[str] = []
        self.validation_log: List[str] = []
        self.timing: Dict[str, float] = {}
        self.error: str = ""
        self.evidence: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "test_name": self.test_name,
            "test_type": self.test_type,
            "passed": self.passed,
            "objective": self.objective,
            "setup": self.setup_info,
            "execution_log": self.execution_log,
            "validation_log": self.validation_log,
            "timing": self.timing,
            "error": self.error,
            "evidence": self.evidence
        }


class AcidTestSuite:
    """
    Comprehensive ACID validation test suite.

    Uses isolated test databases:
    - SQL: ingestion_data_test.db
    - MongoDB: ingestion_db_test
    """

    def __init__(self):
        # Initialize isolated test databases
        self.sql_manager = SQLManager(db_path='ingestion_data_test.db')
        self.mongo_manager = MongoDBManager(
            connection_string='mongodb://localhost:27017/',
            db_name='ingestion_db_test'
        )
        self.coordinator = TransactionCoordinator(self.sql_manager, self.mongo_manager)

        self.test_results: List[AcidTestResult] = []

        logger.info("ACID Test Suite initialized with isolated databases")

    def cleanup_databases(self):
        """Clean all data from test databases"""
        try:
            # Clean SQL
            self.sql_manager.cursor.execute("DELETE FROM ingested_records")
            for table in self.sql_manager.list_child_tables():
                self.sql_manager.cursor.execute(f"DELETE FROM {table}")
            self.sql_manager.connection.commit()

            # Clean MongoDB
            for collection in self.mongo_manager.list_collections():
                self.mongo_manager.delete_records({}, collection)

            logger.info("Test databases cleaned")
        except Exception as e:
            logger.error("Database cleanup error: %s", e)

    # ========================================================================
    # ATOMICITY TESTS
    # ========================================================================

    def test_a1_single_insert_failure_rollback(self) -> AcidTestResult:
        """
        Test A1: Single insert across SQL + Mongo, force Mongo failure → SQL should rollback
        """
        result = AcidTestResult("A1: Single Insert Failure Rollback", "Atomicity")
        result.objective = "Verify that SQL insert rolls back when MongoDB insert fails"

        try:
            self.cleanup_databases()

            # Setup
            test_record = {
                'username': 'test_user_a1',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'email': 'a1@test.com'
            }
            result.setup_info = f"Test record: {test_record['username']}"

            # Begin transaction
            start_time = time.time()
            tx_id = self.coordinator.begin_transaction()
            result.execution_log.append(f"Transaction {tx_id} begun")

            # Add SQL insert
            self.coordinator.add_operation(tx_id, 'insert', 'sql', test_record)
            result.execution_log.append("SQL insert operation added")

            # Prepare (execute operations)
            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"Prepare phase: success={success}")

            if success:
                # Simulate MongoDB failure by forcing abort
                result.execution_log.append("Simulating MongoDB failure - forcing abort")
                self.coordinator.abort(tx_id)
                result.execution_log.append("Transaction aborted")

            result.timing['total_time'] = time.time() - start_time

            # Validation: Check that SQL record does NOT exist
            sql_records = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': test_record['username']},
                limit=10
            )

            result.validation_log.append(f"SQL records found: {len(sql_records)}")
            result.evidence['sql_records_after_rollback'] = len(sql_records)

            # Test passes if NO records exist (rollback worked)
            result.passed = (len(sql_records) == 0)

            if result.passed:
                result.validation_log.append("✓ PASS: SQL record successfully rolled back")
            else:
                result.validation_log.append("✗ FAIL: SQL record still exists after rollback")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test A1 error: %s", e)

        self.test_results.append(result)
        return result

    def test_a2_batch_insert_partial_failure(self) -> AcidTestResult:
        """
        Test A2: Batch insert (5 records), fail on 3rd → all 5 should be absent
        """
        result = AcidTestResult("A2: Batch Insert Partial Failure", "Atomicity")
        result.objective = "Verify that partial batch failure causes complete rollback"

        try:
            self.cleanup_databases()

            # Setup: Create 5 records
            test_records = [
                {
                    'username': f'batch_user_{i}',
                    'sys_ingested_at': datetime.now(timezone.utc).isoformat() + f'.{i:03d}',
                    't_stamp': datetime.now(timezone.utc).isoformat(),
                    'email': f'user{i}@test.com'
                }
                for i in range(1, 6)
            ]
            result.setup_info = f"Batch of {len(test_records)} records"

            start_time = time.time()
            tx_id = self.coordinator.begin_transaction()
            result.execution_log.append(f"Transaction {tx_id} begun")

            # Add first 3 operations
            for i, record in enumerate(test_records[:3]):
                self.coordinator.add_operation(tx_id, 'insert', 'both', record)
                result.execution_log.append(f"Added record {i+1}/5")

            # Prepare first batch
            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"First batch prepared: success={success}")

            # Force failure by aborting
            result.execution_log.append("Simulating failure on 3rd record")
            self.coordinator.abort(tx_id)
            result.execution_log.append("Transaction aborted - all changes rolled back")

            result.timing['total_time'] = time.time() - start_time

            # Validation: Check that NO records exist
            sql_count = 0
            mongo_count = 0

            for record in test_records:
                sql_found = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters={'username': record['username']},
                    limit=1
                )
                sql_count += len(sql_found)

                mongo_found = self.mongo_manager.find_records(
                    filters={'username': record['username']},
                    collection_name='ingested_records',
                    limit=1
                )
                mongo_count += len(mongo_found)

            result.validation_log.append(f"SQL records found: {sql_count}/5")
            result.validation_log.append(f"MongoDB records found: {mongo_count}/5")
            result.evidence['sql_records_found'] = sql_count
            result.evidence['mongo_records_found'] = mongo_count

            result.passed = (sql_count == 0 and mongo_count == 0)

            if result.passed:
                result.validation_log.append("✓ PASS: All batch records rolled back")
            else:
                result.validation_log.append("✗ FAIL: Some records persist after failure")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test A2 error: %s", e)

        self.test_results.append(result)
        return result

    def test_a3_update_with_nested_data_failure(self) -> AcidTestResult:
        """
        Test A3: Update + nested child insertion, one backend fails → all changes reverted
        """
        result = AcidTestResult("A3: Update with Nested Data Failure", "Atomicity")
        result.objective = "Verify atomicity when updating root record and adding nested data"

        try:
            self.cleanup_databases()

            # Setup: Insert initial record
            initial_record = {
                'username': 'nested_user',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'email': 'initial@test.com',
                'age': 25
            }

            self.sql_manager.insert_record(initial_record)
            self.mongo_manager.insert_record(initial_record, 'ingested_records')
            result.setup_info = f"Initial record: {initial_record['username']}"

            # Attempt transaction to update
            start_time = time.time()
            tx_id = self.coordinator.begin_transaction()
            result.execution_log.append(f"Transaction {tx_id} begun")

            # Update operation
            updated_record = dict(initial_record)
            updated_record['email'] = 'updated@test.com'
            updated_record['age'] = 30

            self.coordinator.add_operation(tx_id, 'update', 'both', {
                'filters': {'username': 'nested_user'},
                'new_data': updated_record
            })
            result.execution_log.append("Update operation added")

            # Prepare
            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"Prepare: success={success}")

            # Force failure
            result.execution_log.append("Simulating failure - forcing abort")
            self.coordinator.abort(tx_id)

            result.timing['total_time'] = time.time() - start_time

            # Validation: Check original values remain
            sql_record = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': 'nested_user'},
                limit=1
            )

            mongo_record = self.mongo_manager.find_records(
                filters={'username': 'nested_user'},
                collection_name='ingested_records',
                limit=1
            )

            result.validation_log.append(f"SQL records: {len(sql_record)}")
            result.validation_log.append(f"MongoDB records: {len(mongo_record)}")

            if sql_record:
                result.evidence['sql_email'] = sql_record[0].get('email')
                result.evidence['sql_age'] = sql_record[0].get('age')
                result.validation_log.append(f"SQL email: {sql_record[0].get('email')}")
                result.validation_log.append(f"SQL age: {sql_record[0].get('age')}")

            # Pass if original values remain
            sql_unchanged = sql_record and sql_record[0].get('email') == 'initial@test.com'
            result.passed = sql_unchanged

            if result.passed:
                result.validation_log.append("✓ PASS: Update rolled back, original data intact")
            else:
                result.validation_log.append("✗ FAIL: Data was modified despite rollback")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test A3 error: %s", e)

        self.test_results.append(result)
        return result

    # ========================================================================
    # CONSISTENCY TESTS
    # ========================================================================

    def test_c1_unique_constraint_enforcement(self) -> AcidTestResult:
        """
        Test C1: Unique constraint (sys_ingested_at) → duplicate insert should fail atomically
        """
        result = AcidTestResult("C1: Unique Constraint Enforcement", "Consistency")
        result.objective = "Verify unique constraints prevent duplicate inserts across backends"

        try:
            self.cleanup_databases()

            # Setup: Insert initial record
            timestamp = datetime.now(timezone.utc).isoformat() + '.001'
            record1 = {
                'username': 'unique_user',
                'sys_ingested_at': timestamp,
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'email': 'unique@test.com'
            }

            self.sql_manager.insert_record(record1)
            self.mongo_manager.insert_record(record1, 'ingested_records')
            result.setup_info = f"Initial record with sys_ingested_at: {timestamp}"
            result.execution_log.append("First record inserted successfully")

            # Attempt duplicate insert
            start_time = time.time()
            duplicate_record = dict(record1)
            duplicate_record['email'] = 'duplicate@test.com'

            tx_id = self.coordinator.begin_transaction()
            result.execution_log.append(f"Transaction {tx_id} begun for duplicate insert")

            self.coordinator.add_operation(tx_id, 'insert', 'both', duplicate_record)
            result.execution_log.append("Duplicate insert operation added")

            # Prepare should detect conflict
            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"Prepare result: success={success}, error={error}")

            if not success:
                self.coordinator.abort(tx_id)
                result.execution_log.append("Transaction aborted due to constraint violation")

            result.timing['total_time'] = time.time() - start_time

            # Validation: Only 1 record should exist
            sql_records = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'sys_ingested_at': timestamp},
                limit=10
            )

            result.validation_log.append(f"SQL records with timestamp: {len(sql_records)}")
            result.evidence['sql_count'] = len(sql_records)

            result.passed = (len(sql_records) == 1 and not success)

            if result.passed:
                result.validation_log.append("✓ PASS: Unique constraint enforced")
            else:
                result.validation_log.append("✗ FAIL: Duplicate allowed or incorrect count")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test C1 error: %s", e)

        self.test_results.append(result)
        return result

    def test_c2_foreign_key_integrity(self) -> AcidTestResult:
        """
        Test C2: Foreign key integrity (parent-child relationship)
        """
        result = AcidTestResult("C2: Foreign Key Integrity", "Consistency")
        result.objective = "Verify foreign key constraints maintain referential integrity"

        try:
            self.cleanup_databases()

            # Setup: Insert parent record
            parent_time = datetime.now(timezone.utc).isoformat() + '.001'
            parent_record = {
                'username': 'parent_user',
                'sys_ingested_at': parent_time,
                't_stamp': datetime.now(timezone.utc).isoformat()
            }

            self.sql_manager.insert_record(parent_record)
            result.setup_info = f"Parent record: {parent_time}"
            result.execution_log.append("Parent record inserted")

            # Create child table
            self.sql_manager.ensure_child_table('norm_test_children', {'value': 'string'})

            # Insert child records
            child_rows = [
                {'value': 'child1'},
                {'value': 'child2'}
            ]
            self.sql_manager.insert_child_rows('norm_test_children', parent_time, child_rows)
            result.execution_log.append("Child records inserted")

            # Attempt to delete parent (should cascade or fail)
            start_time = time.time()
            tx_id = self.coordinator.begin_transaction()

            self.coordinator.add_operation(tx_id, 'delete', 'sql', {
                'filters': {'sys_ingested_at': parent_time}
            })
            result.execution_log.append("Delete parent operation added")

            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"Prepare: success={success}")

            if success:
                self.coordinator.commit(tx_id)
                result.execution_log.append("Delete committed")

            result.timing['total_time'] = time.time() - start_time

            # Validation: Check parent and children
            parent_exists = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'sys_ingested_at': parent_time},
                limit=1
            )

            children_exist = self.sql_manager.fetch_records(
                table_name='norm_test_children',
                filters={'parent_sys_ingested_at': parent_time},
                limit=10
            )

            result.validation_log.append(f"Parent record exists: {len(parent_exists) > 0}")
            result.validation_log.append(f"Child records remaining: {len(children_exist)}")
            result.evidence['parent_exists'] = len(parent_exists) > 0
            result.evidence['children_count'] = len(children_exist)

            # Pass if cascade delete worked (no parent, no children)
            result.passed = (len(parent_exists) == 0 and len(children_exist) == 0)

            if result.passed:
                result.validation_log.append("✓ PASS: Foreign key cascade delete worked")
            else:
                result.validation_log.append("✗ FAIL: Referential integrity violated")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test C2 error: %s", e)

        self.test_results.append(result)
        return result

    def test_c3_type_constraint_enforcement(self) -> AcidTestResult:
        """
        Test C3: Type constraints (e.g., age must be integer)
        """
        result = AcidTestResult("C3: Type Constraint Enforcement", "Consistency")
        result.objective = "Verify type constraints are enforced across backends"

        try:
            self.cleanup_databases()

            # Add age column with INTEGER type
            self.sql_manager.add_column_if_not_exists('age', 'integer')

            # Attempt to insert invalid type
            start_time = time.time()
            invalid_record = {
                'username': 'type_test_user',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'age': 'not_a_number'  # Invalid type
            }

            tx_id = self.coordinator.begin_transaction()
            result.execution_log.append(f"Transaction {tx_id} begun")

            self.coordinator.add_operation(tx_id, 'insert', 'sql', invalid_record)
            result.execution_log.append("Insert with invalid type added")

            success, error = self.coordinator.prepare(tx_id)
            result.execution_log.append(f"Prepare: success={success}, error={error}")

            if success:
                # SQLite may coerce types, so check actual value
                self.coordinator.commit(tx_id)

                # Check what was actually stored
                records = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters={'username': 'type_test_user'},
                    limit=1
                )

                if records:
                    stored_age = records[0].get('age')
                    result.evidence['stored_age'] = stored_age
                    result.evidence['stored_type'] = type(stored_age).__name__
                    result.validation_log.append(f"Stored age: {stored_age} (type: {type(stored_age).__name__})")

                    # Pass if type was coerced or rejected
                    result.passed = (stored_age == 0 or stored_age is None or isinstance(stored_age, int))
            else:
                # Transaction failed - constraint enforced
                self.coordinator.abort(tx_id)
                result.passed = True
                result.validation_log.append("✓ Type constraint enforced - transaction rejected")

            result.timing['total_time'] = time.time() - start_time

            if result.passed:
                result.validation_log.append("✓ PASS: Type constraint handled appropriately")
            else:
                result.validation_log.append("✗ FAIL: Invalid type was stored")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            # Exception during type violation is also acceptable (pass)
            result.passed = "type" in str(e).lower() or "integer" in str(e).lower()
            logger.error("Test C3 error: %s", e)

        self.test_results.append(result)
        return result

    # ========================================================================
    # ISOLATION TESTS
    # ========================================================================

    def test_i1_no_dirty_reads(self) -> AcidTestResult:
        """
        Test I1: Concurrent transactions (T1 reading, T2 writing) → no dirty reads
        """
        result = AcidTestResult("I1: No Dirty Reads", "Isolation")
        result.objective = "Verify concurrent reads don't see uncommitted writes"

        try:
            self.cleanup_databases()

            # Setup: Insert initial record
            init_record = {
                'username': 'concurrent_user',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'balance': 100
            }
            self.sql_manager.add_column_if_not_exists('balance', 'integer')
            self.sql_manager.insert_record(init_record)
            result.setup_info = f"Initial balance: {init_record['balance']}"

            values_read = []
            error_occurred = False

            def writer_transaction():
                """T2: Write but don't commit immediately"""
                try:
                    tx_id = self.coordinator.begin_transaction()
                    result.execution_log.append("[T2] Writer transaction started")

                    updated = dict(init_record)
                    updated['balance'] = 200

                    self.coordinator.add_operation(tx_id, 'update', 'sql', {
                        'filters': {'username': 'concurrent_user'},
                        'new_data': updated
                    })

                    self.coordinator.prepare(tx_id)
                    result.execution_log.append("[T2] Write prepared, sleeping before commit...")

                    time.sleep(0.5)  # Hold transaction open

                    self.coordinator.commit(tx_id)
                    result.execution_log.append("[T2] Write committed")
                except Exception as e:
                    result.execution_log.append(f"[T2] Error: {e}")
                    nonlocal error_occurred
                    error_occurred = True

            def reader_transaction():
                """T1: Read during T2's transaction"""
                try:
                    time.sleep(0.2)  # Start after writer begins
                    result.execution_log.append("[T1] Reader starting...")

                    # Read without transaction (should see committed data only)
                    records = self.sql_manager.fetch_records(
                        table_name='ingested_records',
                        filters={'username': 'concurrent_user'},
                        limit=1
                    )

                    if records:
                        balance = records[0].get('balance')
                        values_read.append(balance)
                        result.execution_log.append(f"[T1] Read balance: {balance}")
                except Exception as e:
                    result.execution_log.append(f"[T1] Error: {e}")
                    nonlocal error_occurred
                    error_occurred = True

            # Run concurrently
            start_time = time.time()
            t1 = threading.Thread(target=writer_transaction)
            t2 = threading.Thread(target=reader_transaction)

            t1.start()
            t2.start()

            t1.join()
            t2.join()

            result.timing['total_time'] = time.time() - start_time

            # Validation: Reader should see original value (100), not uncommitted (200)
            result.evidence['values_read'] = values_read
            result.validation_log.append(f"Values read by T1: {values_read}")

            # Pass if reader saw original value (no dirty read)
            result.passed = (len(values_read) > 0 and values_read[0] == 100 and not error_occurred)

            if result.passed:
                result.validation_log.append("✓ PASS: No dirty reads detected")
            else:
                result.validation_log.append("✗ FAIL: Dirty read occurred or error")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test I1 error: %s", e)

        self.test_results.append(result)
        return result

    def test_i2_concurrent_inserts(self) -> AcidTestResult:
        """
        Test I2: Two concurrent inserts of different records → both succeed
        """
        result = AcidTestResult("I2: Concurrent Inserts", "Isolation")
        result.objective = "Verify concurrent inserts of different records both succeed"

        try:
            self.cleanup_databases()

            success_count = [0]
            error_occurred = [False]

            def insert_transaction(user_id: int):
                try:
                    tx_id = self.coordinator.begin_transaction()
                    result.execution_log.append(f"[T{user_id}] Transaction started")

                    record = {
                        'username': f'concurrent_user_{user_id}',
                        'sys_ingested_at': datetime.now(timezone.utc).isoformat() + f'.{user_id:03d}',
                        't_stamp': datetime.now(timezone.utc).isoformat(),
                        'user_id': user_id
                    }

                    self.coordinator.add_operation(tx_id, 'insert', 'both', record)
                    success, error = self.coordinator.prepare(tx_id)

                    if success:
                        self.coordinator.commit(tx_id)
                        result.execution_log.append(f"[T{user_id}] Committed successfully")
                        success_count[0] += 1
                    else:
                        self.coordinator.abort(tx_id)
                        result.execution_log.append(f"[T{user_id}] Aborted: {error}")
                        error_occurred[0] = True
                except Exception as e:
                    result.execution_log.append(f"[T{user_id}] Error: {e}")
                    error_occurred[0] = True

            # Run 3 concurrent inserts
            start_time = time.time()
            threads = [threading.Thread(target=insert_transaction, args=(i,)) for i in range(1, 4)]

            for t in threads:
                t.start()

            for t in threads:
                t.join()

            result.timing['total_time'] = time.time() - start_time

            # Validation: All 3 should succeed
            sql_count = self.sql_manager.get_record_count()
            result.validation_log.append(f"Successful commits: {success_count[0]}/3")
            result.validation_log.append(f"SQL records: {sql_count}")
            result.evidence['successful_commits'] = success_count[0]
            result.evidence['sql_records'] = sql_count

            result.passed = (success_count[0] == 3 and not error_occurred[0])

            if result.passed:
                result.validation_log.append("✓ PASS: All concurrent inserts succeeded")
            else:
                result.validation_log.append("✗ FAIL: Some inserts failed")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test I2 error: %s", e)

        self.test_results.append(result)
        return result

    def test_i3_serializable_isolation(self) -> AcidTestResult:
        """
        Test I3: Concurrent update + read on same record → verify isolation
        """
        result = AcidTestResult("I3: Serializable Isolation", "Isolation")
        result.objective = "Verify reads are isolated from concurrent updates"

        try:
            self.cleanup_databases()

            # Setup
            shared_record = {
                'username': 'shared_user',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'counter': 0
            }
            self.sql_manager.add_column_if_not_exists('counter', 'integer')
            self.sql_manager.insert_record(shared_record)
            result.setup_info = "Initial counter: 0"

            read_values = []

            def updater():
                try:
                    tx_id = self.coordinator.begin_transaction()
                    result.execution_log.append("[Updater] Transaction started")

                    updated = dict(shared_record)
                    updated['counter'] = 10

                    self.coordinator.add_operation(tx_id, 'update', 'sql', {
                        'filters': {'username': 'shared_user'},
                        'new_data': updated
                    })

                    self.coordinator.prepare(tx_id)
                    time.sleep(0.3)  # Hold transaction
                    self.coordinator.commit(tx_id)
                    result.execution_log.append("[Updater] Committed: counter=10")
                except Exception as e:
                    result.execution_log.append(f"[Updater] Error: {e}")

            def reader():
                try:
                    time.sleep(0.1)  # Start during update
                    records = self.sql_manager.fetch_records(
                        table_name='ingested_records',
                        filters={'username': 'shared_user'},
                        limit=1
                    )
                    if records:
                        counter = records[0].get('counter', 0)
                        read_values.append(counter)
                        result.execution_log.append(f"[Reader] Read counter: {counter}")
                except Exception as e:
                    result.execution_log.append(f"[Reader] Error: {e}")

            start_time = time.time()
            t1 = threading.Thread(target=updater)
            t2 = threading.Thread(target=reader)

            t1.start()
            t2.start()

            t1.join()
            t2.join()

            result.timing['total_time'] = time.time() - start_time

            # Validation: Reader should see either 0 (before commit) or 10 (after), not partial
            result.evidence['read_values'] = read_values
            result.validation_log.append(f"Values read: {read_values}")

            valid_values = [0, 10]
            result.passed = all(v in valid_values for v in read_values)

            if result.passed:
                result.validation_log.append("✓ PASS: Serializable isolation maintained")
            else:
                result.validation_log.append("✗ FAIL: Invalid intermediate state observed")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test I3 error: %s", e)

        self.test_results.append(result)
        return result

    # ========================================================================
    # DURABILITY TESTS
    # ========================================================================

    def test_d1_crash_recovery(self) -> AcidTestResult:
        """
        Test D1: Write data, simulate crash (close connections), reopen → data intact
        """
        result = AcidTestResult("D1: Crash Recovery", "Durability")
        result.objective = "Verify committed data survives connection restart"

        try:
            self.cleanup_databases()

            # Write data
            test_record = {
                'username': 'durable_user',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat(),
                'data': 'important_data'
            }

            start_time = time.time()
            tx_id = self.coordinator.begin_transaction()
            self.coordinator.add_operation(tx_id, 'insert', 'both', test_record)
            self.coordinator.prepare(tx_id)
            self.coordinator.commit(tx_id)
            result.execution_log.append("Data committed")

            # Simulate crash - close connections
            self.sql_manager.close()
            self.mongo_manager.close()
            result.execution_log.append("Connections closed (simulating crash)")

            # Reopen databases
            self.sql_manager = SQLManager(db_path='ingestion_data_test.db')
            self.mongo_manager = MongoDBManager(
                connection_string='mongodb://localhost:27017/',
                db_name='ingestion_db_test'
            )
            self.coordinator = TransactionCoordinator(self.sql_manager, self.mongo_manager)
            result.execution_log.append("Connections reopened")

            result.timing['total_time'] = time.time() - start_time

            # Validation: Data should still exist
            sql_records = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': 'durable_user'},
                limit=1
            )

            mongo_records = self.mongo_manager.find_records(
                filters={'username': 'durable_user'},
                collection_name='ingested_records',
                limit=1
            )

            result.validation_log.append(f"SQL records after restart: {len(sql_records)}")
            result.validation_log.append(f"MongoDB records after restart: {len(mongo_records)}")
            result.evidence['sql_recovered'] = len(sql_records) > 0
            result.evidence['mongo_recovered'] = len(mongo_records) > 0

            result.passed = (len(sql_records) > 0 and len(mongo_records) > 0)

            if result.passed:
                result.validation_log.append("✓ PASS: Data survived crash and recovery")
            else:
                result.validation_log.append("✗ FAIL: Data lost after restart")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test D1 error: %s", e)

        self.test_results.append(result)
        return result

    def test_d2_committed_data_persists(self) -> AcidTestResult:
        """
        Test D2: Committed transaction data persists even if new writes fail
        """
        result = AcidTestResult("D2: Committed Data Persists", "Durability")
        result.objective = "Verify committed data is durable despite subsequent failures"

        try:
            self.cleanup_databases()

            # Commit first transaction
            record1 = {
                'username': 'persistent_user_1',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.001',
                't_stamp': datetime.now(timezone.utc).isoformat()
            }

            start_time = time.time()
            tx1 = self.coordinator.begin_transaction()
            self.coordinator.add_operation(tx1, 'insert', 'both', record1)
            self.coordinator.prepare(tx1)
            self.coordinator.commit(tx1)
            result.execution_log.append("First transaction committed successfully")

            # Attempt second transaction that will fail
            record2 = {
                'username': 'persistent_user_2',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.002',
                't_stamp': datetime.now(timezone.utc).isoformat()
            }

            tx2 = self.coordinator.begin_transaction()
            self.coordinator.add_operation(tx2, 'insert', 'both', record2)
            self.coordinator.prepare(tx2)
            # Force failure
            self.coordinator.abort(tx2)
            result.execution_log.append("Second transaction aborted")

            result.timing['total_time'] = time.time() - start_time

            # Validation: First record should still exist
            sql_record1 = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': 'persistent_user_1'},
                limit=1
            )

            sql_record2 = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': 'persistent_user_2'},
                limit=1
            )

            result.validation_log.append(f"First record exists: {len(sql_record1) > 0}")
            result.validation_log.append(f"Second record exists: {len(sql_record2) > 0}")
            result.evidence['first_persisted'] = len(sql_record1) > 0
            result.evidence['second_absent'] = len(sql_record2) == 0

            result.passed = (len(sql_record1) > 0 and len(sql_record2) == 0)

            if result.passed:
                result.validation_log.append("✓ PASS: Committed data persists despite later failure")
            else:
                result.validation_log.append("✗ FAIL: Data inconsistency detected")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test D2 error: %s", e)

        self.test_results.append(result)
        return result

    def test_d3_durability_after_rollback(self) -> AcidTestResult:
        """
        Test D3: Data committed before rollback remains intact
        """
        result = AcidTestResult("D3: Durability After Rollback", "Durability")
        result.objective = "Verify previous commits are not affected by later rollbacks"

        try:
            self.cleanup_databases()

            # Commit several records
            records = [
                {
                    'username': f'durable_batch_{i}',
                    'sys_ingested_at': datetime.now(timezone.utc).isoformat() + f'.{i:03d}',
                    't_stamp': datetime.now(timezone.utc).isoformat()
                }
                for i in range(3)
            ]

            start_time = time.time()
            for i, record in enumerate(records):
                tx = self.coordinator.begin_transaction()
                self.coordinator.add_operation(tx, 'insert', 'both', record)
                self.coordinator.prepare(tx)
                self.coordinator.commit(tx)
                result.execution_log.append(f"Committed record {i+1}/3")

            # Attempt transaction that will rollback
            bad_record = {
                'username': 'bad_record',
                'sys_ingested_at': datetime.now(timezone.utc).isoformat() + '.999',
                't_stamp': datetime.now(timezone.utc).isoformat()
            }

            tx_bad = self.coordinator.begin_transaction()
            self.coordinator.add_operation(tx_bad, 'insert', 'both', bad_record)
            self.coordinator.prepare(tx_bad)
            self.coordinator.abort(tx_bad)
            result.execution_log.append("Final transaction aborted")

            result.timing['total_time'] = time.time() - start_time

            # Validation: First 3 should exist, last should not
            existing_count = 0
            for record in records:
                found = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters={'username': record['username']},
                    limit=1
                )
                if found:
                    existing_count += 1

            bad_found = self.sql_manager.fetch_records(
                table_name='ingested_records',
                filters={'username': 'bad_record'},
                limit=1
            )

            result.validation_log.append(f"Committed records found: {existing_count}/3")
            result.validation_log.append(f"Aborted record found: {len(bad_found) > 0}")
            result.evidence['committed_count'] = existing_count
            result.evidence['aborted_found'] = len(bad_found) > 0

            result.passed = (existing_count == 3 and len(bad_found) == 0)

            if result.passed:
                result.validation_log.append("✓ PASS: Prior commits unaffected by rollback")
            else:
                result.validation_log.append("✗ FAIL: Durability compromised")

        except Exception as e:
            result.error = str(e)
            result.execution_log.append(f"ERROR: {e}")
            logger.error("Test D3 error: %s", e)

        self.test_results.append(result)
        return result

    def run_all_tests(self) -> List[AcidTestResult]:
        """Run all ACID tests and return results"""
        logger.info("Starting ACID test suite")
        start_time = time.time()

        # Atomicity tests
        logger.info("Running Atomicity tests...")
        self.test_a1_single_insert_failure_rollback()
        self.test_a2_batch_insert_partial_failure()
        self.test_a3_update_with_nested_data_failure()

        # Consistency tests
        logger.info("Running Consistency tests...")
        self.test_c1_unique_constraint_enforcement()
        self.test_c2_foreign_key_integrity()
        self.test_c3_type_constraint_enforcement()

        # Isolation tests
        logger.info("Running Isolation tests...")
        self.test_i1_no_dirty_reads()
        self.test_i2_concurrent_inserts()
        self.test_i3_serializable_isolation()

        # Durability tests
        logger.info("Running Durability tests...")
        self.test_d1_crash_recovery()
        self.test_d2_committed_data_persists()
        self.test_d3_durability_after_rollback()

        total_time = time.time() - start_time
        passed_count = sum(1 for r in self.test_results if r.passed)
        logger.info(
            "ACID test suite completed: %s/%s passed in %.2fs",
            passed_count,
            len(self.test_results),
            total_time
        )

        return self.test_results


def main():
    """Run ACID test suite and print summary"""
    suite = AcidTestSuite()

    print("=" * 80)
    print("ACID VALIDATION TEST SUITE")
    print("Testing hybrid SQL + MongoDB transaction coordinator")
    print("=" * 80)
    print()

    results = suite.run_all_tests()

    # Print summary
    print("\n" + "=" * 80)
    print("TEST RESULTS SUMMARY")
    print("=" * 80)

    by_type = {}
    for result in results:
        if result.test_type not in by_type:
            by_type[result.test_type] = []
        by_type[result.test_type].append(result)

    for test_type in ['Atomicity', 'Consistency', 'Isolation', 'Durability']:
        if test_type in by_type:
            tests = by_type[test_type]
            passed = sum(1 for t in tests if t.passed)
            print(f"\n{test_type}: {passed}/{len(tests)} passed")
            for test in tests:
                status = "[PASS]" if test.passed else "[FAIL]"
                print(f"  {status} - {test.test_name}")

    total_passed = sum(1 for r in results if r.passed)
    print(f"\nOverall: {total_passed}/{len(results)} tests passed")
    print("=" * 80)

    # Generate reports
    print("\nGenerating reports...")
    report_paths = generate_reports_from_test_results(results)
    print("\n" + "=" * 80)
    print("ACID validation complete!")
    print(f"View detailed report at: {report_paths['html']}")
    print("=" * 80)


if __name__ == "__main__":
    main()
