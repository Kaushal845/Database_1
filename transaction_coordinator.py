"""
Transaction Coordinator - Ensures ACID properties across SQL and MongoDB backends

This module implements a 2-phase commit protocol for hybrid transactions:
1. PREPARE phase: Validate and stage operations on both backends
2. COMMIT phase: Finalize all changes or ABORT on any failure
3. ROLLBACK: Undo all changes if any backend fails

Key Features:
- Atomic operations across SQL and MongoDB
- Transaction isolation with unique transaction IDs
- Full rollback capability on failure
- Transaction state tracking
"""
import threading
import time
import sqlite3
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from enum import Enum
from logging_utils import get_logger


logger = get_logger("transaction_coordinator")


class TransactionState(Enum):
    """Transaction lifecycle states"""
    PENDING = "pending"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"
    FAILED = "failed"


class TransactionOperation:
    """Represents a single operation within a transaction"""

    def __init__(self, op_type: str, backend: str, data: Dict[str, Any]):
        self.op_type = op_type  # insert, update, delete
        self.backend = backend  # sql, mongo, both
        self.data = data
        self.rollback_data: Optional[Dict[str, Any]] = None


class Transaction:
    """Represents a distributed transaction across SQL + MongoDB"""

    def __init__(self, tx_id: str):
        self.tx_id = tx_id
        self.state = TransactionState.PENDING
        self.operations: List[TransactionOperation] = []
        self.created_at = datetime.now(timezone.utc)
        self.committed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self.sql_savepoint: Optional[str] = None
        self.mongo_temp_records: List[str] = []  # Track temp IDs for rollback


class TransactionCoordinator:
    """
    Coordinates ACID transactions across SQL (SQLite) and MongoDB backends.

    Two-phase commit protocol:
    1. Begin: Create transaction context, start SQL transaction
    2. Prepare: Validate operations, track rollback info
    3. Commit: Finalize all changes atomically
    4. Abort: Rollback all changes if any backend fails
    """

    def __init__(self, sql_manager, mongo_manager):
        self.sql_manager = sql_manager
        self.mongo_manager = mongo_manager
        self.active_transactions: Dict[str, Transaction] = {}
        self.transaction_counter = 0
        self.lock = threading.RLock()

        # Initialize transaction tracking collection in MongoDB
        self._init_transaction_tracking()

        logger.info("Transaction coordinator initialized")

    def _init_transaction_tracking(self):
        """Create MongoDB collection for transaction metadata"""
        try:
            self.mongo_manager._get_collection('_transactions').create_index('tx_id', unique=True)
            self.mongo_manager._get_collection('_transaction_temp').create_index('tx_id')
            logger.info("Transaction tracking collections initialized")
        except Exception as e:
            logger.warning("Transaction tracking init warning: %s", e)

    def begin_transaction(self) -> str:
        """
        Begin a new distributed transaction.

        Returns:
            tx_id: Unique transaction identifier
        """
        with self.lock:
            self.transaction_counter += 1
            tx_id = f"tx_{int(time.time() * 1000)}_{self.transaction_counter}"

            # Create transaction object
            tx = Transaction(tx_id)

            # Begin SQL transaction
            try:
                # Ensure we're not already in a transaction (cleanup from previous errors)
                if self.sql_manager.connection_in_transaction:
                    logger.warning("Cleaning up previous transaction state")
                    try:
                        self.sql_manager.connection.rollback()
                    except:
                        pass
                    self.sql_manager.connection_in_transaction = False
                    self.sql_manager.in_transaction = False

                # Set both transaction flags
                self.sql_manager.in_transaction = True
                self.sql_manager.connection_in_transaction = True

                # SQLite specific: start transaction with savepoint
                tx.sql_savepoint = f"sp_{tx_id}"
                self.sql_manager.cursor.execute("BEGIN IMMEDIATE")
                self.sql_manager.cursor.execute(f"SAVEPOINT {tx.sql_savepoint}")
                logger.debug("SQL transaction started: %s", tx_id)
            except Exception as e:
                self.sql_manager.in_transaction = False
                self.sql_manager.connection_in_transaction = False
                logger.error("Failed to begin SQL transaction: %s", e)
                raise

            # Track transaction in MongoDB
            try:
                self.mongo_manager.insert_record({
                    'tx_id': tx_id,
                    'state': tx.state.value,
                    'created_at': tx.created_at.isoformat(),
                    'operations': []
                }, collection_name='_transactions')
                logger.debug("MongoDB transaction tracking started: %s", tx_id)
            except Exception as e:
                # Rollback SQL if MongoDB tracking fails
                self.sql_manager.cursor.execute("ROLLBACK")
                logger.error("Failed to track transaction in MongoDB: %s", e)
                raise

            self.active_transactions[tx_id] = tx
            logger.info("Transaction %s begun successfully", tx_id)
            return tx_id

    def add_operation(
        self,
        tx_id: str,
        op_type: str,
        backend: str,
        data: Dict[str, Any]
    ) -> bool:
        """
        Add an operation to the transaction without executing it.

        Args:
            tx_id: Transaction ID
            op_type: Operation type (insert, update, delete)
            backend: Target backend (sql, mongo, both)
            data: Operation data

        Returns:
            True if operation added successfully
        """
        with self.lock:
            tx = self.active_transactions.get(tx_id)
            if not tx:
                logger.error("Transaction %s not found", tx_id)
                return False

            if tx.state != TransactionState.PENDING:
                logger.error("Cannot add operation to transaction in state %s", tx.state)
                return False

            op = TransactionOperation(op_type, backend, data)
            tx.operations.append(op)
            logger.debug("Operation added to transaction %s: %s on %s", tx_id, op_type, backend)
            return True

    def _prepare_sql_operation(
        self,
        tx: Transaction,
        op: TransactionOperation
    ) -> Tuple[bool, Optional[str]]:
        """
        Prepare SQL operation within transaction.

        Returns:
            (success, error_message)
        """
        try:
            if op.op_type == 'insert':
                # Store original state for rollback
                sys_ingested_at = op.data.get('sys_ingested_at')

                # Check if record already exists (for rollback tracking)
                existing = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters={'sys_ingested_at': sys_ingested_at},
                    limit=1
                )
                op.rollback_data = {'existing': existing[0] if existing else None}

                # Execute insert
                success = self.sql_manager.insert_record(op.data)
                if not success:
                    return False, "SQL insert failed"

            elif op.op_type == 'update':
                # Store original state for rollback
                filters = op.data.get('filters', {})
                original_records = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters=filters,
                    limit=1000
                )
                op.rollback_data = {'original_records': original_records}

                # Only update fields that actually exist as SQL columns.
                # MongoDB-only fields (e.g. `country`, `profile`) will be handled
                # by _prepare_mongo_operation and should be silently ignored here.
                new_data = op.data.get('new_data', {})
                existing_columns = self.sql_manager.get_existing_columns('ingested_records')
                update_fields = {}
                for field, value in new_data.items():
                    safe_field = self.sql_manager._sanitize_identifier(field)
                    if safe_field in existing_columns:
                        update_fields[safe_field] = value
                    # else: field lives in MongoDB — skip here, handled by Mongo prepare

                # Execute SQL partial UPDATE (no initial-value check — allow setting new values)
                if update_fields:
                    success = self._execute_sql_update(filters, update_fields)
                    if not success:
                        return False, "SQL update failed"


            elif op.op_type == 'delete':
                # Store original state for rollback
                filters = op.data.get('filters', {})
                original_records = self.sql_manager.fetch_records(
                    table_name='ingested_records',
                    filters=filters,
                    limit=1000
                )
                op.rollback_data = {'original_records': original_records}

                # Execute delete
                deleted = self.sql_manager.delete_records('ingested_records', filters)
                if deleted == 0:
                    logger.warning("SQL delete affected 0 rows")

            return True, None

        except Exception as e:
            logger.error("SQL operation prepare failed: %s", e)
            return False, str(e)

    def _execute_sql_update(self, filters: Dict[str, Any], update_fields: Dict[str, Any]) -> bool:
        """
        Execute SQL UPDATE statement.

        Args:
            filters: WHERE conditions
            update_fields: SET field = value pairs

        Returns:
            True if successful
        """
        try:
            # Build WHERE clause
            where_parts = []
            where_values = []
            for key, value in filters.items():
                safe_key = self.sql_manager._sanitize_identifier(key)
                where_parts.append(f"{safe_key} = ?")
                where_values.append(value)

            where_clause = " AND ".join(where_parts) if where_parts else "1=1"

            # Build SET clause
            set_parts = []
            set_values = []
            for key, value in update_fields.items():
                set_parts.append(f"{key} = ?")
                set_values.append(value)

            set_clause = ", ".join(set_parts)

            query = f"UPDATE ingested_records SET {set_clause} WHERE {where_clause}"

            self.sql_manager.cursor.execute(query, set_values + where_values)
            if not self.sql_manager.in_transaction:
                self.sql_manager.connection.commit()
            return True

        except Exception as e:
            logger.error("SQL update failed: %s", e)
            return False

    def _prepare_mongo_operation(
        self,
        tx: Transaction,
        op: TransactionOperation
    ) -> Tuple[bool, Optional[str]]:
        """
        Prepare MongoDB operation within transaction.

        Returns:
            (success, error_message)
        """
        try:
            if op.op_type == 'insert':
                # Insert with transaction marker
                record = dict(op.data)
                record['_tx_id'] = tx.tx_id
                record['_tx_temp'] = True  # Mark as temporary until commit

                success = self.mongo_manager.insert_record(
                    record,
                    collection_name='_transaction_temp'
                )
                if not success:
                    return False, "MongoDB insert to temp failed"

                # Track for rollback
                tx.mongo_temp_records.append(record.get('sys_ingested_at', ''))

            elif op.op_type == 'update':
                # Store original state
                filters = op.data.get('filters', {})
                original_docs = self.mongo_manager.find_records(
                    filters=filters,
                    collection_name='ingested_records',
                    limit=1000
                )
                op.rollback_data = {'original_docs': original_docs}

                # Stage the partial update — no initial-value check (allow adding new fields)
                new_data = op.data.get('new_data', {})

                # Stage update in temp collection for 2-phase commit
                success = self.mongo_manager.insert_record({
                    '_tx_id': tx.tx_id,
                    '_tx_operation': 'update',
                    '_tx_filters': filters,
                    '_tx_data': new_data
                }, collection_name='_transaction_temp')

                if not success:
                    return False, "MongoDB update prepare failed"

            elif op.op_type == 'delete':
                # Store original state
                filters = op.data.get('filters', {})
                original_docs = self.mongo_manager.find_records(
                    filters=filters,
                    collection_name='ingested_records',
                    limit=1000
                )
                op.rollback_data = {'original_docs': original_docs}

                # Mark deletes as pending
                success = self.mongo_manager.insert_record({
                    '_tx_id': tx.tx_id,
                    '_tx_operation': 'delete',
                    '_tx_filters': filters
                }, collection_name='_transaction_temp')

                if not success:
                    return False, "MongoDB delete prepare failed"

            return True, None

        except Exception as e:
            logger.error("MongoDB operation prepare failed: %s", e)
            return False, str(e)

    def prepare(self, tx_id: str) -> Tuple[bool, Optional[str]]:
        """
        Prepare phase: Validate and stage all operations.

        Returns:
            (success, error_message)
        """
        with self.lock:
            tx = self.active_transactions.get(tx_id)
            if not tx:
                return False, f"Transaction {tx_id} not found"

            if tx.state != TransactionState.PENDING:
                return False, f"Transaction in invalid state: {tx.state.value}"

            logger.info("Preparing transaction %s with %s operations", tx_id, len(tx.operations))

            # Prepare each operation
            for op in tx.operations:
                if op.backend in ('sql', 'both'):
                    success, error = self._prepare_sql_operation(tx, op)
                    if not success:
                        tx.error = f"SQL prepare failed: {error}"
                        return False, tx.error

                if op.backend in ('mongo', 'both'):
                    success, error = self._prepare_mongo_operation(tx, op)
                    if not success:
                        tx.error = f"MongoDB prepare failed: {error}"
                        return False, tx.error

            tx.state = TransactionState.PREPARED
            logger.info("Transaction %s prepared successfully", tx_id)
            return True, None

    def commit(self, tx_id: str) -> Tuple[bool, Optional[str]]:
        """
        Commit phase: Finalize all changes atomically.

        Returns:
            (success, error_message)
        """
        with self.lock:
            tx = self.active_transactions.get(tx_id)
            if not tx:
                return False, f"Transaction {tx_id} not found"

            if tx.state != TransactionState.PREPARED:
                return False, f"Transaction not prepared: {tx.state.value}"

            logger.info("Committing transaction %s", tx_id)

            try:
                # Commit SQL changes
                if tx.sql_savepoint:
                    self.sql_manager.cursor.execute(f"RELEASE SAVEPOINT {tx.sql_savepoint}")
                self.sql_manager.connection.commit()
                logger.debug("SQL transaction committed: %s", tx_id)

                # Move MongoDB temp records to main collection
                temp_records = self.mongo_manager.find_records(
                    filters={'_tx_id': tx_id},
                    collection_name='_transaction_temp',
                    limit=10000
                )

                for record in temp_records:
                    operation = record.get('_tx_operation')

                    if operation == 'update':
                        # Execute pending partial update using $set so all other fields are preserved.
                        # The old code did delete+insert which wiped the entire document.
                        clean_data = {k: v for k, v in record['_tx_data'].items() if not k.startswith('_tx_')}
                        self.mongo_manager.update_records(
                            record['_tx_filters'],
                            clean_data,
                            'ingested_records'
                        )

                    elif operation == 'delete':
                        # Execute pending delete
                        self.mongo_manager.delete_records(
                            record['_tx_filters'],
                            'ingested_records'
                        )

                    else:
                        # Regular insert: move to main collection
                        clean_record = {k: v for k, v in record.items() if not k.startswith('_tx_')}
                        if clean_record:
                            self.mongo_manager.insert_record(clean_record, 'ingested_records')

                # Clean up temp records
                self.mongo_manager.delete_records({'_tx_id': tx_id}, '_transaction_temp')
                logger.debug("MongoDB transaction committed: %s", tx_id)

                # Update transaction state
                tx.state = TransactionState.COMMITTED
                tx.committed_at = datetime.now(timezone.utc)

                self.mongo_manager.delete_records({'tx_id': tx_id}, '_transactions')
                logger.info("Transaction %s committed successfully", tx_id)

                # Clear both transaction flags
                self.sql_manager.in_transaction = False
                self.sql_manager.connection_in_transaction = False

                # Cleanup
                del self.active_transactions[tx_id]

                return True, None

            except Exception as e:
                logger.error("Commit failed for transaction %s: %s", tx_id, e)
                tx.error = str(e)
                # Attempt rollback
                self.abort(tx_id)
                return False, f"Commit failed: {e}"

    def abort(self, tx_id: str) -> Tuple[bool, Optional[str]]:
        """
        Abort transaction: Rollback all changes.

        Returns:
            (success, error_message)
        """
        with self.lock:
            tx = self.active_transactions.get(tx_id)
            if not tx:
                return False, f"Transaction {tx_id} not found"

            logger.info("Aborting transaction %s", tx_id)

            try:
                # Rollback SQL changes
                if tx.sql_savepoint:
                    self.sql_manager.cursor.execute(f"ROLLBACK TO SAVEPOINT {tx.sql_savepoint}")
                    self.sql_manager.cursor.execute(f"RELEASE SAVEPOINT {tx.sql_savepoint}")
                self.sql_manager.connection.rollback()
                logger.debug("SQL transaction rolled back: %s", tx_id)

                # Clean up MongoDB temp records
                self.mongo_manager.delete_records({'_tx_id': tx_id}, '_transaction_temp')
                logger.debug("MongoDB temp records cleaned: %s", tx_id)

                # Update transaction state
                tx.state = TransactionState.ABORTED
                self.mongo_manager.delete_records({'tx_id': tx_id}, '_transactions')

                logger.info("Transaction %s aborted successfully", tx_id)

                # Clear both transaction flags
                self.sql_manager.in_transaction = False
                self.sql_manager.connection_in_transaction = False

                # Cleanup
                del self.active_transactions[tx_id]

                return True, None

            except Exception as e:
                logger.error("Abort failed for transaction %s: %s", tx_id, e)
                self.sql_manager.in_transaction = False  # Clear flag even on failure
                self.sql_manager.connection_in_transaction = False
                tx.state = TransactionState.FAILED
                return False, f"Abort failed: {e}"

    def get_transaction_status(self, tx_id: str) -> Optional[Dict[str, Any]]:
        """Get current transaction status"""
        with self.lock:
            tx = self.active_transactions.get(tx_id)
            if not tx:
                return None

            return {
                'tx_id': tx.tx_id,
                'state': tx.state.value,
                'operations_count': len(tx.operations),
                'created_at': tx.created_at.isoformat(),
                'committed_at': tx.committed_at.isoformat() if tx.committed_at else None,
                'error': tx.error
            }

    def list_active_transactions(self) -> List[Dict[str, Any]]:
        """List all active transactions"""
        with self.lock:
            return [
                self.get_transaction_status(tx_id)
                for tx_id in self.active_transactions.keys()
            ]
