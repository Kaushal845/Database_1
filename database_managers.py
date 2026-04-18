"""
Database Managers - Handle SQL and MongoDB connections and operations
"""
import sqlite3
import threading
from collections import defaultdict
from typing import Dict, Any, List, Optional, Iterable
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from type_detector import TypeDetector
from logging_utils import get_logger


sql_logger = get_logger("sql")
mongo_logger = get_logger("mongo")


class InMemoryInsertResult:
    def __init__(self, inserted_id: str):
        self.inserted_id = inserted_id


class InMemoryDeleteResult:
    def __init__(self, deleted_count: int):
        self.deleted_count = deleted_count


class InMemoryCollection:
    """Simple in-memory Mongo-like collection for offline/testing use."""

    def __init__(self, name: str):
        self.name = name
        self.documents: List[Dict[str, Any]] = []

    def create_index(self, *args, **kwargs):
        return None

    def insert_one(self, document: Dict[str, Any]) -> InMemoryInsertResult:
        doc = dict(document)
        if "_id" not in doc:
            doc["_id"] = f"mem_{self.name}_{len(self.documents) + 1}"
        self.documents.append(doc)
        return InMemoryInsertResult(str(doc["_id"]))

    def _matches(self, doc: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        for key, value in filters.items():
            if key not in doc:
                return False
            if isinstance(value, dict):
                if "$in" in value:
                    if doc[key] not in value.get("$in", []):
                        return False
                    continue
                if "$starts_with" in value:
                    prefix = str(value.get("$starts_with", ""))
                    if not str(doc[key]).lower().startswith(prefix.lower()):
                        return False
                    continue
                if "$regex" in value:
                    import re
                    pattern = value.get("$regex", "")
                    options = value.get("$options", "")
                    flags = re.IGNORECASE if "i" in str(options).lower() else 0
                    if not re.search(pattern, str(doc[key]), flags):
                        return False
                    continue
            elif doc[key] != value:
                return False
        return True

    def find_one(self, filters: Optional[Dict[str, Any]] = None):
        search = filters or {}
        for doc in self.documents:
            if self._matches(doc, search):
                return dict(doc)
        return None

    def find(
        self,
        filters: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        search = filters or {}
        results = [dict(doc) for doc in self.documents if self._matches(doc, search)]
        if not projection:
            return results

        include = [key for key, value in projection.items() if value]
        if not include:
            return results

        projected = []
        for doc in results:
            projected.append({key: doc[key] for key in include if key in doc})
        return projected

    def delete_many(self, filters: Optional[Dict[str, Any]] = None) -> InMemoryDeleteResult:
        search = filters or {}
        kept: List[Dict[str, Any]] = []
        deleted = 0
        for doc in self.documents:
            if self._matches(doc, search):
                deleted += 1
            else:
                kept.append(doc)
        self.documents = kept
        return InMemoryDeleteResult(deleted)

    def count_documents(self, filters: Optional[Dict[str, Any]] = None) -> int:
        search = filters or {}
        return len([doc for doc in self.documents if self._matches(doc, search)])


class SQLManager:
    """
    Manages SQLite database with dynamic schema evolution.
    Automatically creates/updates tables based on field placements.
    """
    
    def __init__(self, db_path='ingestion_data.db'):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._connection_lock = threading.RLock()  # Lock for thread-safe operations

        # Create a separate read-only connection for isolation
        # This ensures reads don't see uncommitted writes from active transactions
        self._read_connection = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._read_connection.row_factory = sqlite3.Row
        self._read_lock = threading.RLock()

        self.connection.execute("PRAGMA foreign_keys = ON")
        # Enable WAL mode for better concurrent access and isolation
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA read_uncommitted = 0")  # Disable dirty reads
        # Set synchronous to NORMAL for better performance with WAL
        self.connection.execute("PRAGMA synchronous = NORMAL")

        # Also set WAL mode on read connection
        self._read_connection.execute("PRAGMA journal_mode = WAL")
        self._read_connection.execute("PRAGMA read_uncommitted = 0")
        self._read_connection.execute("PRAGMA synchronous = NORMAL")

        self.connection.row_factory = sqlite3.Row
        self.cursor = self.connection.cursor()
        self.type_detector = TypeDetector()

        # Use thread-local storage for per-thread transaction state
        # This ensures each thread knows if IT is in a transaction
        self._local = threading.local()

        # Global flag to track if ANY transaction is active on the connection
        # This is needed because only one transaction can be active at a time
        self._connection_in_transaction = False

        self._initialize_schema()

    @property
    def in_transaction(self) -> bool:
        """Get thread-local transaction state"""
        return getattr(self._local, 'in_transaction', False)

    @in_transaction.setter
    def in_transaction(self, value: bool):
        """Set thread-local transaction state"""
        self._local.in_transaction = value

    @property
    def connection_in_transaction(self) -> bool:
        """Get global connection transaction state"""
        return self._connection_in_transaction

    @connection_in_transaction.setter
    def connection_in_transaction(self, value: bool):
        """Set global connection transaction state"""
        self._connection_in_transaction = value
    
    def _initialize_schema(self):
        """Initialize the base SQL table with mandatory fields"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS ingested_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            sys_ingested_at TIMESTAMP NOT NULL,
            t_stamp TEXT,
            UNIQUE(sys_ingested_at)
        )
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    @staticmethod
    def _sanitize_identifier(identifier: str) -> str:
        sanitized = "".join(char if (char.isalnum() or char == "_") else "_" for char in identifier)
        if not sanitized:
            sanitized = "field"
        if sanitized[0].isdigit():
            sanitized = f"f_{sanitized}"
        return sanitized

    @staticmethod
    def _escape_like(value: str) -> str:
        """Escape SQL LIKE wildcards while using backslash as escape char."""
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    
    def get_existing_columns(self, table_name: str = "ingested_records") -> List[str]:
        """
        Get list of all existing columns in a table.

        Args:
            table_name: Name of the table to check

        Returns:
            List of column names
        """
        try:
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in self.cursor.fetchall()]
        except Exception as e:
            sql_logger.error("Error getting columns for table %s: %s", table_name, e)
            return []

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        try:
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            return self.cursor.fetchone() is not None
        except Exception as e:
            sql_logger.error("Error checking table existence %s: %s", table_name, e)
            return False

    def add_column_if_not_exists(self, column_name: str, data_type: str, unique: bool = False):
        """
        Dynamically add a column to the table if it doesn't exist.
        """
        try:
            # Check if column exists
            existing_columns = self.get_existing_columns("ingested_records")

            if column_name not in existing_columns:
                sql_type = self.type_detector.get_sql_type(data_type)
                safe_column = self._sanitize_identifier(column_name)
                alter_query = f"ALTER TABLE ingested_records ADD COLUMN {safe_column} {sql_type}"
                self.cursor.execute(alter_query)
                self.connection.commit()
                sql_logger.info("Added column %s (%s)", safe_column, sql_type)
                
                # Add unique constraint if needed (requires separate index)
                if unique and column_name not in ['username', 't_stamp']:
                    try:
                        index_query = f"CREATE UNIQUE INDEX idx_{safe_column} ON ingested_records({safe_column})"
                        self.cursor.execute(index_query)
                        self.connection.commit()
                        sql_logger.info("Added unique index on %s", safe_column)
                    except sqlite3.IntegrityError:
                        sql_logger.warning(
                            "Could not add unique index on %s because duplicate values exist",
                            safe_column,
                        )
        
        except Exception as e:
            sql_logger.error("Error adding column %s: %s", column_name, e)
    
    def _validate_type(self, column_name: str, value: Any) -> bool:
        """
        Validate that a value matches the expected SQL type for a column.

        Args:
            column_name: Name of the column
            value: Value to validate

        Returns:
            True if type is valid, False otherwise
        """
        if value is None:
            return True  # NULL is allowed for any type

        # Get column type from schema
        self.cursor.execute("PRAGMA table_info(ingested_records)")
        columns = {row[1]: row[2].upper() for row in self.cursor.fetchall()}

        expected_type = columns.get(column_name, "").upper()
        if not expected_type:
            return True  # Column doesn't exist yet, will be created

        # Validate based on SQLite type
        if "INT" in expected_type:
            if not isinstance(value, (int, bool)):
                sql_logger.error("Type mismatch for column %s: expected integer, got %s", column_name, type(value).__name__)
                return False
        elif "REAL" in expected_type or "FLOAT" in expected_type or "DOUBLE" in expected_type:
            if not isinstance(value, (int, float, bool)):
                sql_logger.error("Type mismatch for column %s: expected numeric, got %s", column_name, type(value).__name__)
                return False
        elif "TEXT" in expected_type or "CHAR" in expected_type or "CLOB" in expected_type:
            if not isinstance(value, str):
                sql_logger.error("Type mismatch for column %s: expected text, got %s", column_name, type(value).__name__)
                return False
        # BLOB and other types are more permissive

        return True

    def insert_record(self, record: Dict[str, Any]) -> bool:
        """
        Insert a record into SQL database.

        Args:
            record: Dictionary with field names and values (already filtered for SQL)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure mandatory fields exist
            if 'sys_ingested_at' not in record:
                record['sys_ingested_at'] = datetime.now(timezone.utc).isoformat()

            # Build dynamic INSERT query
            normalized = {}
            for key, value in record.items():
                # Filter out MongoDB-specific fields that start with underscore
                if not key.startswith('_'):
                    safe_key = self._sanitize_identifier(key)
                    # Validate type before adding to normalized dict
                    if not self._validate_type(safe_key, value):
                        sql_logger.error("Type validation failed for field %s", key)
                        return False
                    normalized[safe_key] = value

            columns = list(normalized.keys())
            placeholders = ['?' for _ in columns]
            values = [normalized[col] for col in columns]

            query = f"""
            INSERT INTO ingested_records ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """

            self.cursor.execute(query, values)
            if not self.in_transaction:
                self.connection.commit()
            return True

        except sqlite3.IntegrityError as e:
            sql_logger.warning("Integrity error inserting SQL record: %s", e)
            return False
        except Exception as e:
            sql_logger.error("Insert error: %s", e)
            return False
    
    def get_record_count(self) -> int:
        """Get total number of records in SQL"""
        self.cursor.execute("SELECT COUNT(*) FROM ingested_records")
        return self.cursor.fetchone()[0]

    def ensure_child_table(self, table_name: str, columns: Dict[str, str]):
        """Create child table for normalized one-to-many entities."""
        safe_table = self._sanitize_identifier(table_name)

        create_query = f"""
        CREATE TABLE IF NOT EXISTS {safe_table} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_sys_ingested_at TEXT NOT NULL,
            item_index INTEGER,
            FOREIGN KEY(parent_sys_ingested_at) REFERENCES ingested_records(sys_ingested_at) ON DELETE CASCADE
        )
        """
        self.cursor.execute(create_query)

        self.cursor.execute(f"PRAGMA table_info({safe_table})")
        existing_columns = [row[1] for row in self.cursor.fetchall()]

        for column_name, semantic_type in columns.items():
            safe_column = self._sanitize_identifier(column_name)
            if safe_column in existing_columns:
                continue
            sql_type = self.type_detector.get_sql_type(semantic_type)
            self.cursor.execute(f"ALTER TABLE {safe_table} ADD COLUMN {safe_column} {sql_type}")

        self.cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{safe_table}_parent ON {safe_table}(parent_sys_ingested_at)"
        )
        self.connection.commit()

    def insert_child_rows(self, table_name: str, parent_sys_ingested_at: str, rows: List[Dict[str, Any]]):
        """Insert normalized child rows for one parent record."""
        safe_table = self._sanitize_identifier(table_name)
        for index, row in enumerate(rows):
            payload = {
                "parent_sys_ingested_at": parent_sys_ingested_at,
                "item_index": index,
            }
            payload.update({self._sanitize_identifier(k): v for k, v in row.items()})

            columns = list(payload.keys())
            placeholders = ["?" for _ in columns]
            values = [payload[column] for column in columns]

            query = (
                f"INSERT INTO {safe_table} ({', '.join(columns)}) "
                f"VALUES ({', '.join(placeholders)})"
            )
            self.cursor.execute(query, values)

        if not self.in_transaction:
            self.connection.commit()

    def fetch_records(
        self,
        table_name: str,
        fields: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Fetch records from any SQL table with simple equality filters."""
        safe_table = self._sanitize_identifier(table_name)
        selected_columns = "*"
        if fields:
            safe_fields = [self._sanitize_identifier(field) for field in fields]
            selected_columns = ", ".join(safe_fields)

        query = f"SELECT {selected_columns} FROM {safe_table}"
        values: List[Any] = []
        if filters:
            clauses = []
            for key, value in filters.items():
                safe_key = self._sanitize_identifier(key)

                if isinstance(value, dict):
                    if "$starts_with" in value:
                        prefix = self._escape_like(str(value.get("$starts_with", "")))
                        clauses.append(f"{safe_key} LIKE ? ESCAPE '\\'")
                        values.append(f"{prefix}%")
                        continue

                    if "$in" in value:
                        candidates = list(value.get("$in", []))
                        if not candidates:
                            clauses.append("1 = 0")
                        else:
                            placeholders = ", ".join(["?" for _ in candidates])
                            clauses.append(f"{safe_key} IN ({placeholders})")
                            values.extend(candidates)
                        continue

                clauses.append(f"{safe_key} = ?")
                values.append(value)
            query += " WHERE " + " AND ".join(clauses)

        query += " LIMIT ?"
        values.append(limit)

        # Use read-only connection when not in a transaction to ensure isolation
        # This prevents seeing uncommitted writes from active transactions
        if not self.in_transaction:
            # Use separate read connection with explicit transaction for isolation
            with self._read_lock:
                try:
                    # Start a DEFERRED read transaction to get a consistent snapshot
                    try:
                        self._read_connection.execute("BEGIN DEFERRED")
                    except sqlite3.OperationalError as e:
                        # Already in transaction, just proceed
                        if "already" not in str(e).lower():
                            raise

                    cursor = self._read_connection.execute(query, values)
                    results = [dict(row) for row in cursor.fetchall()]

                    # Commit the read transaction
                    try:
                        self._read_connection.commit()
                    except sqlite3.OperationalError:
                        # No transaction to commit
                        pass

                    return results
                except Exception as e:
                    # Rollback on error
                    try:
                        self._read_connection.rollback()
                    except:
                        pass
                    sql_logger.error("Read error: %s", e)
                    # Return empty on error to be safe
                    return []
        else:
            # Within a managed transaction, use the main connection
            cursor = self.connection.execute(query, values)
            return [dict(row) for row in cursor.fetchall()]

    def update_root_field(self, sys_ingested_at: str, field_name: str, value: Any) -> bool:
        """Update a single field in the root SQL table for one ingested record."""
        safe_field = self._sanitize_identifier(field_name)
        try:
            query = f"UPDATE ingested_records SET {safe_field} = ? WHERE sys_ingested_at = ?"
            self.cursor.execute(query, (value, sys_ingested_at))
            if not self.in_transaction:
                self.connection.commit()
            return self.cursor.rowcount > 0
        except Exception as error:
            sql_logger.error("Update error for field '%s': %s", safe_field, error)
            return False

    def delete_records(self, table_name: str, filters: Dict[str, Any]) -> int:
        """Delete records with simple equality filters and return deleted count."""
        safe_table = self._sanitize_identifier(table_name)
        clauses = []
        values: List[Any] = []
        for key, value in filters.items():
            clauses.append(f"{self._sanitize_identifier(key)} = ?")
            values.append(value)

        where_clause = " AND ".join(clauses) if clauses else "1 = 1"
        self.cursor.execute(f"DELETE FROM {safe_table} WHERE {where_clause}", values)
        deleted_count = self.cursor.rowcount
        if not self.in_transaction:
            self.connection.commit()
        return deleted_count

    def update_records(self, table_name: str, filters: Dict[str, Any], updates: Dict[str, Any]) -> int:
        """
        Update records in the table matching filters with the given updates.

        Args:
            table_name: Name of the table
            filters: WHERE conditions
            updates: SET field = value pairs

        Returns:
            Number of rows updated
        """
        try:
            safe_table = self._sanitize_identifier(table_name)

            # Build WHERE clause
            where_clauses = []
            where_values = []
            for key, value in filters.items():
                safe_key = self._sanitize_identifier(key)
                where_clauses.append(f"{safe_key} = ?")
                where_values.append(value)

            where_clause = " AND ".join(where_clauses) if where_clauses else "1 = 1"

            # Build SET clause
            set_clauses = []
            set_values = []
            for key, value in updates.items():
                safe_key = self._sanitize_identifier(key)
                set_clauses.append(f"{safe_key} = ?")
                set_values.append(value)

            set_clause = ", ".join(set_clauses)

            query = f"UPDATE {safe_table} SET {set_clause} WHERE {where_clause}"

            self.cursor.execute(query, set_values + where_values)
            updated_count = self.cursor.rowcount
            if not self.in_transaction:
                self.connection.commit()
            return updated_count

        except Exception as e:
            sql_logger.error("Update error: %s", e)
            return 0

    def list_child_tables(self) -> List[str]:
        """List user tables excluding the root table."""
        query = (
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'ingested_records'"
        )
        rows = self.connection.execute(query).fetchall()
        return [row[0] for row in rows]
    
    def get_schema(self) -> List[tuple]:
        """Get current table schema"""
        self.cursor.execute("PRAGMA table_info(ingested_records)")
        return self.cursor.fetchall()
    
    def close(self):
        """Close database connection"""
        if hasattr(self, '_read_connection'):
            self._read_connection.close()
        self.connection.close()


class MongoDBManager:
    """
    Manages MongoDB connection for flexible/nested data storage.
    """
    
    def __init__(self, connection_string=None, db_name='ingestion_db'):
        import os
        if connection_string is None:
            mongo_host = os.environ.get('MONGO_HOST', 'localhost')
            mongo_port = os.environ.get('MONGO_PORT', '27017')
            connection_string = f'mongodb://{mongo_host}:{mongo_port}/'
        self._memory_collections: Dict[str, InMemoryCollection] = {}
        self.using_memory_fallback = False

        try:
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            self.collection = self.db['ingested_records']
            self._initialize_indexes()
            mongo_logger.info("Connected successfully (database=%s)", db_name)
        except ConnectionFailure as e:
            mongo_logger.warning("Connection failed: %s", e)
            mongo_logger.warning("Using in-memory fallback collections")
            self.using_memory_fallback = True
            self.client = None
            self.db = None
            self.collection = self._get_collection('ingested_records')

    def _get_collection(self, collection_name: str):
        if self.using_memory_fallback:
            if collection_name not in self._memory_collections:
                self._memory_collections[collection_name] = InMemoryCollection(collection_name)
            return self._memory_collections[collection_name]

        return self.db[collection_name]
    
    def _initialize_indexes(self):
        """Create indexes on mandatory fields"""
        if self.collection is not None:
            try:
                # Index on sys_ingested_at for joining with SQL
                self.collection.create_index('sys_ingested_at', unique=True)
                # Index on username for queries
                self.collection.create_index('username')
                mongo_logger.info("Indexes created on ingested_records")
            except Exception as e:
                mongo_logger.error("Index creation error: %s", e)
    
    def insert_record(self, record: Dict[str, Any], collection_name: str = 'ingested_records') -> bool:
        """
        Insert a record into MongoDB.
        
        Args:
            record: Dictionary with field names and values (already filtered for MongoDB)
        
        Returns:
            True if successful, False otherwise
        """
        collection = self._get_collection(collection_name)
        if collection is None:
            return False
        
        try:
            # Ensure mandatory fields exist
            if 'sys_ingested_at' not in record:
                record['sys_ingested_at'] = datetime.now(timezone.utc).isoformat()
            
            collection.insert_one(record)
            return True
        
        except Exception as e:
            mongo_logger.error("Insert error into %s: %s", collection_name, e)
            return False
    
    def get_record_count(self, collection_name: str = 'ingested_records') -> int:
        """Get total number of records in MongoDB"""
        collection = self._get_collection(collection_name)
        if collection is None:
            return 0
        try:
            return collection.count_documents({})
        except Exception as e:
            mongo_logger.error("Count error in %s: %s", collection_name, e)
            return 0

    def find_records(
        self,
        filters: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        collection_name: str = 'ingested_records',
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Find documents with optional projection."""
        collection = self._get_collection(collection_name)
        if collection is None:
            return []

        projection = None
        if fields:
            projection = {field: 1 for field in fields}

        cursor = collection.find(filters or {}, projection)
        if isinstance(cursor, list):
            docs = cursor[:limit]
        else:
            docs = list(cursor.limit(limit))

        serialized = []
        for doc in docs:
            item = dict(doc)
            if "_id" in item:
                item["_id"] = str(item["_id"])
            serialized.append(item)
        return serialized

    def delete_records(self, filters: Dict[str, Any], collection_name: str = 'ingested_records') -> int:
        """Delete documents matching filters and return deleted count."""
        collection = self._get_collection(collection_name)
        if collection is None:
            return 0
        result = collection.delete_many(filters)
        return result.deleted_count

    def update_root_field(
        self,
        sys_ingested_at: str,
        field_name: str,
        value: Any,
        collection_name: str = 'ingested_records',
    ) -> bool:
        """Set one field value on a root Mongo document identified by sys_ingested_at."""
        collection = self._get_collection(collection_name)
        if collection is None:
            return False

        if self.using_memory_fallback:
            for index, doc in enumerate(collection.documents):
                if doc.get('sys_ingested_at') == sys_ingested_at:
                    updated = dict(doc)
                    updated[field_name] = value
                    collection.documents[index] = updated
                    return True
            return False

        result = collection.update_one(
            {'sys_ingested_at': sys_ingested_at},
            {'$set': {field_name: value}},
        )
        return result.matched_count > 0

    def remove_buffer_field(
        self,
        sys_ingested_at: str,
        field_name: str,
        collection_name: str = 'buffer_records',
    ) -> bool:
        """Remove one field from a buffered payload and delete the doc when empty."""
        collection = self._get_collection(collection_name)
        if collection is None:
            return False

        if self.using_memory_fallback:
            for index, doc in enumerate(collection.documents):
                if doc.get('sys_ingested_at') != sys_ingested_at:
                    continue

                fields = dict(doc.get('fields', {}))
                if field_name not in fields:
                    return False

                fields.pop(field_name, None)
                if fields:
                    updated = dict(doc)
                    updated['fields'] = fields
                    collection.documents[index] = updated
                else:
                    collection.documents.pop(index)
                return True
            return False

        target = collection.find_one({'sys_ingested_at': sys_ingested_at})
        if not target:
            return False

        fields = target.get('fields', {})
        if field_name not in fields:
            return False

        collection.update_one(
            {'_id': target['_id']},
            {'$unset': {f'fields.{field_name}': ""}},
        )

        updated_target = collection.find_one({'_id': target['_id']})
        if not updated_target or not updated_target.get('fields'):
            collection.delete_one({'_id': target['_id']})

        return True

    def update_records(
        self,
        filters: Dict[str, Any],
        updates: Dict[str, Any],
        collection_name: str = 'ingested_records',
    ) -> int:
        """
        Update records in MongoDB matching filters with the given updates.

        Args:
            filters: Query filters
            updates: Fields to update
            collection_name: Collection name

        Returns:
            Number of documents updated
        """
        collection = self._get_collection(collection_name)
        if collection is None:
            return 0

        if self.using_memory_fallback:
            updated_count = 0
            for index, doc in enumerate(collection.documents):
                if self._matches_filters(doc, filters):
                    updated = dict(doc)
                    updated.update(updates)
                    collection.documents[index] = updated
                    updated_count += 1
            return updated_count

        result = collection.update_many(filters, {'$set': updates})
        return result.modified_count

    def _matches_filters(self, doc: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Check if document matches filters (simple implementation)"""
        for key, value in filters.items():
            doc_value = doc.get(key)
            if isinstance(value, dict):
                if "$in" in value:
                    if doc_value not in value.get("$in", []):
                        return False
                    continue
                if "$starts_with" in value:
                    prefix = str(value.get("$starts_with", ""))
                    if not str(doc_value).lower().startswith(prefix.lower()):
                        return False
                    continue
                if "$regex" in value:
                    import re
                    pattern = value.get("$regex", "")
                    options = value.get("$options", "")
                    flags = re.IGNORECASE if "i" in str(options).lower() else 0
                    if not re.search(pattern, str(doc_value), flags):
                        return False
                    continue
            elif doc_value != value:
                return False
        return True

    def list_collections(self) -> List[str]:
        """List available collections."""
        if self.using_memory_fallback:
            return list(self._memory_collections.keys())
        if self.db is None:
            return []
        return self.db.list_collection_names()
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()


# Example usage
if __name__ == "__main__":
    # Test SQL Manager
    print("Testing SQL Manager:")
    print("-" * 60)
    
    sql_mgr = SQLManager('test_ingestion.db')
    
    # Add some columns
    sql_mgr.add_column_if_not_exists('email', 'email', unique=False)
    sql_mgr.add_column_if_not_exists('age', 'integer')
    sql_mgr.add_column_if_not_exists('device_id', 'uuid', unique=True)
    
    # Insert test record
    test_record = {
        'username': 'testuser',
        'sys_ingested_at': datetime.now(timezone.utc).isoformat(),
        't_stamp': datetime.now(timezone.utc).isoformat(),
        'email': 'test@example.com',
        'age': 25
    }
    
    success = sql_mgr.insert_record(test_record)
    print(f"Insert successful: {success}")
    print(f"Record count: {sql_mgr.get_record_count()}")
    
    # Test MongoDB Manager
    print("\nTesting MongoDB Manager:")
    print("-" * 60)
    
    mongo_mgr = MongoDBManager()
    
    if mongo_mgr.collection is not None:
        test_mongo_record = {
            'username': 'testuser',
            'sys_ingested_at': datetime.now(timezone.utc).isoformat(),
            't_stamp': datetime.now(timezone.utc).isoformat(),
            'metadata': {
                'nested': {
                    'value': 123
                }
            },
            'tags': ['test', 'demo']
        }
        
        success = mongo_mgr.insert_record(test_mongo_record)
        print(f"Insert successful: {success}")
        print(f"Record count: {mongo_mgr.get_record_count()}")
    
    # Cleanup
    sql_mgr.close()
    mongo_mgr.close()

