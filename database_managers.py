"""
Database Managers - Handle SQL and MongoDB connections and operations
"""
import sqlite3
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from type_detector import TypeDetector


class SQLManager:
    """
    Manages SQLite database with dynamic schema evolution.
    Automatically creates/updates tables based on field placements.
    """
    
    def __init__(self, db_path='ingestion_data.db'):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.type_detector = TypeDetector()
        self._initialize_schema()
    
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
    
    def add_column_if_not_exists(self, column_name: str, data_type: str, unique: bool = False):
        """
        Dynamically add a column to the table if it doesn't exist.
        """
        try:
            # Check if column exists
            self.cursor.execute("PRAGMA table_info(ingested_records)")
            existing_columns = [row[1] for row in self.cursor.fetchall()]
            
            if column_name not in existing_columns:
                sql_type = self.type_detector.get_sql_type(data_type)
                alter_query = f"ALTER TABLE ingested_records ADD COLUMN {column_name} {sql_type}"
                self.cursor.execute(alter_query)
                self.connection.commit()
                print(f"[SQL] Added column: {column_name} ({sql_type})")
                
                # Add unique constraint if needed (requires separate index)
                if unique and column_name not in ['username', 't_stamp']:
                    try:
                        index_query = f"CREATE UNIQUE INDEX idx_{column_name} ON ingested_records({column_name})"
                        self.cursor.execute(index_query)
                        self.connection.commit()
                        print(f"[SQL] Added unique index on: {column_name}")
                    except sqlite3.IntegrityError:
                        print(f"[SQL] Could not add unique constraint on {column_name} - duplicate values exist")
        
        except Exception as e:
            print(f"[SQL] Error adding column {column_name}: {e}")
    
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
                record['sys_ingested_at'] = datetime.utcnow().isoformat()
            
            # Build dynamic INSERT query
            columns = list(record.keys())
            placeholders = ['?' for _ in columns]
            values = [record[col] for col in columns]
            
            query = f"""
            INSERT INTO ingested_records ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
            
            self.cursor.execute(query, values)
            self.connection.commit()
            return True
        
        except sqlite3.IntegrityError as e:
            print(f"[SQL] Integrity error: {e}")
            return False
        except Exception as e:
            print(f"[SQL] Insert error: {e}")
            return False
    
    def get_record_count(self) -> int:
        """Get total number of records in SQL"""
        self.cursor.execute("SELECT COUNT(*) FROM ingested_records")
        return self.cursor.fetchone()[0]
    
    def get_schema(self) -> List[tuple]:
        """Get current table schema"""
        self.cursor.execute("PRAGMA table_info(ingested_records)")
        return self.cursor.fetchall()
    
    def close(self):
        """Close database connection"""
        self.connection.close()


class MongoDBManager:
    """
    Manages MongoDB connection for flexible/nested data storage.
    """
    
    def __init__(self, connection_string='mongodb://localhost:27017/', db_name='ingestion_db'):
        try:
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[db_name]
            self.collection = self.db['ingested_records']
            self._initialize_indexes()
            print("[MongoDB] Connected successfully")
        except ConnectionFailure as e:
            print(f"[MongoDB] Connection failed: {e}")
            print("[MongoDB] Continuing without MongoDB - data will only go to SQL")
            self.client = None
            self.db = None
            self.collection = None
    
    def _initialize_indexes(self):
        """Create indexes on mandatory fields"""
        if self.collection is not None:
            try:
                # Index on sys_ingested_at for joining with SQL
                self.collection.create_index('sys_ingested_at', unique=True)
                # Index on username for queries
                self.collection.create_index('username')
                print("[MongoDB] Indexes created")
            except Exception as e:
                print(f"[MongoDB] Index creation error: {e}")
    
    def insert_record(self, record: Dict[str, Any]) -> bool:
        """
        Insert a record into MongoDB.
        
        Args:
            record: Dictionary with field names and values (already filtered for MongoDB)
        
        Returns:
            True if successful, False otherwise
        """
        if self.collection is None:
            return False
        
        try:
            # Ensure mandatory fields exist
            if 'sys_ingested_at' not in record:
                record['sys_ingested_at'] = datetime.utcnow().isoformat()
            
            self.collection.insert_one(record)
            return True
        
        except Exception as e:
            print(f"[MongoDB] Insert error: {e}")
            return False
    
    def get_record_count(self) -> int:
        """Get total number of records in MongoDB"""
        if self.collection is None:
            return 0
        try:
            return self.collection.count_documents({})
        except Exception as e:
            print(f"[MongoDB] Count error: {e}")
            return 0
    
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
        'sys_ingested_at': datetime.utcnow().isoformat(),
        't_stamp': datetime.utcnow().isoformat(),
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
            'sys_ingested_at': datetime.utcnow().isoformat(),
            't_stamp': datetime.utcnow().isoformat(),
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
