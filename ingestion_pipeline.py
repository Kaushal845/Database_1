"""
Data Ingestion Pipeline - Orchestrates the entire ingestion process
Autonomous system that determines SQL vs MongoDB placement dynamically
"""
from typing import Dict, Any, List
from datetime import datetime
import json

from metadata_store import MetadataStore
from field_normalizer import FieldNormalizer
from type_detector import TypeDetector
from placement_heuristics import PlacementHeuristics
from database_managers import SQLManager, MongoDBManager


class IngestionPipeline:
    """
    Autonomous data ingestion system that:
    1. Normalizes field names
    2. Tracks field frequency and type stability
    3. Decides SQL vs MongoDB placement dynamically
    4. Handles bi-temporal timestamps
    5. Maintains traceability via username
    """
    
    def __init__(self, 
                 metadata_file='metadata_store.json',
                 sql_db='ingestion_data.db',
                 mongo_uri='mongodb://localhost:27017/',
                 mongo_db='ingestion_db'):
        
        # Initialize components
        self.metadata_store = MetadataStore(metadata_file)
        self.field_normalizer = FieldNormalizer()
        self.type_detector = TypeDetector()
        self.placement_heuristics = PlacementHeuristics(self.metadata_store)
        
        # Initialize databases
        self.sql_manager = SQLManager(sql_db)
        self.mongo_manager = MongoDBManager(mongo_uri, mongo_db)
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'sql_inserts': 0,
            'mongodb_inserts': 0,
            'errors': 0,
            'fields_discovered': 0
        }
        
        print("[Pipeline] Initialized successfully")
        print(f"[Pipeline] Loaded {len(self.metadata_store.metadata['fields'])} known fields")
    
    def _flatten_record(self, record: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Flatten nested structures for processing.
        Keeps track of both flat and nested representations.
        """
        flat_record = {}
        nested_fields = {}
        
        for key, value in record.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict):
                # Recursively flatten
                nested_fields[new_key] = value
                flattened, _ = self._flatten_record(value, new_key, sep)  # Unpack tuple correctly
                flat_record.update(flattened)
            elif isinstance(value, list):
                # Store lists as-is (MongoDB will handle them)
                flat_record[new_key] = value
                nested_fields[new_key] = value
            else:
                flat_record[new_key] = value
        
        return flat_record, nested_fields if parent_key == '' else {}
    
    def _normalize_and_track(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize field names and track statistics.
        Returns: Dictionary with normalized keys
        """
        normalized_record = {}
        
        for original_key, value in record.items():
            # Normalize the key
            normalized_key = self.field_normalizer.normalize(original_key)
            
            # Store normalization rule if new
            if original_key != normalized_key:
                existing_norm = self.metadata_store.get_normalized_key(original_key)
                if existing_norm == original_key:  # Not yet recorded
                    self.metadata_store.add_normalization_rule(original_key, normalized_key)
            
            # Detect type
            detected_type = self.type_detector.detect_type(value)
            
            # Update field statistics
            self.metadata_store.update_field_stats(normalized_key, detected_type, value)
            
            normalized_record[normalized_key] = value
        
        return normalized_record
    
    def _add_temporal_timestamps(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add bi-temporal timestamps:
        1. t_stamp: Client timestamp (from JSON or current time if missing)
        2. sys_ingested_at: Server timestamp (unique, generated here)
        """
        # Server timestamp (unique identifier for this record)
        sys_timestamp = datetime.utcnow().isoformat() + f".{self.stats['total_processed']:06d}"
        record['sys_ingested_at'] = sys_timestamp
        
        # Client timestamp (preserve if exists, otherwise use current)
        if 't_stamp' not in record and 'timestamp' not in record:
            record['t_stamp'] = datetime.utcnow().isoformat()
        elif 'timestamp' in record and 't_stamp' not in record:
            record['t_stamp'] = record['timestamp']
        
        return record
    
    def _split_by_placement(self, record: Dict[str, Any]) -> tuple:
        """
        Split record into SQL and MongoDB portions based on placement decisions.
        
        Returns: (sql_record, mongo_record)
        """
        sql_record = {}
        mongo_record = {}
        
        # Mandatory fields go to both
        mandatory_fields = ['username', 'sys_ingested_at', 't_stamp']
        
        for field_name, value in record.items():
            placement = self.placement_heuristics.decide_placement(field_name)
            
            if placement == 'SQL' or placement == 'Both':
                # Handle nested structures for SQL (flatten or convert to JSON)
                if isinstance(value, (dict, list)):
                    sql_record[field_name] = json.dumps(value)
                else:
                    sql_record[field_name] = value
                
                # Dynamically add column if needed
                detected_type = self.type_detector.detect_type(value)
                is_unique = self.placement_heuristics.should_be_unique(field_name)
                self.sql_manager.add_column_if_not_exists(field_name, detected_type, is_unique)
            
            if placement == 'MongoDB' or placement == 'Both':
                # MongoDB handles nested structures natively
                mongo_record[field_name] = value
        
        # Ensure mandatory fields are in both
        for field in mandatory_fields:
            if field in record:
                if field not in sql_record:
                    sql_record[field] = record[field]
                if field not in mongo_record:
                    mongo_record[field] = record[field]
        
        return sql_record, mongo_record
    
    def ingest_record(self, raw_record: Dict[str, Any]) -> bool:
        """
        Ingest a single JSON record through the complete pipeline.
        
        Steps:
        1. Flatten nested structures
        2. Normalize field names
        3. Track statistics
        4. Add bi-temporal timestamps
        5. Decide placement (SQL/MongoDB)
        6. Insert into appropriate backend(s)
        
        Returns: True if successful, False otherwise
        """
        try:
            # Step 1: Flatten nested structures for tracking
            flat_record, nested_fields = self._flatten_record(raw_record)
            
            # Step 2: Normalize field names and track statistics
            normalized_record = self._normalize_and_track(flat_record)
            
            # Preserve original nested structure for MongoDB
            for nested_key, nested_value in nested_fields.items():
                normalized_nested_key = self.field_normalizer.normalize(nested_key)
                normalized_record[normalized_nested_key] = nested_value
            
            # Step 3: Add bi-temporal timestamps
            normalized_record = self._add_temporal_timestamps(normalized_record)
            
            # Step 4: Increment record count
            self.metadata_store.increment_total_records()
            
            # Step 5: Split record by placement
            sql_record, mongo_record = self._split_by_placement(normalized_record)
            
            # Step 6: Insert into databases
            sql_success = False
            mongo_success = False
            
            if sql_record and len(sql_record) > 0:
                sql_success = self.sql_manager.insert_record(sql_record)
                if sql_success:
                    self.stats['sql_inserts'] += 1
            
            if mongo_record and len(mongo_record) > 0:
                mongo_success = self.mongo_manager.insert_record(mongo_record)
                if mongo_success:
                    self.stats['mongodb_inserts'] += 1
            
            # Update statistics
            self.stats['total_processed'] += 1
            
            # Periodic metadata save (every 10 records)
            if self.stats['total_processed'] % 10 == 0:
                self.metadata_store.save()
            
            # Log progress every 50 records
            if self.stats['total_processed'] % 50 == 0:
                print(f"[Pipeline] Processed {self.stats['total_processed']} records "
                      f"(SQL: {self.stats['sql_inserts']}, MongoDB: {self.stats['mongodb_inserts']})")
            
            return sql_success or mongo_success
        
        except Exception as e:
            print(f"[Pipeline] Error ingesting record: {e}")
            self.stats['errors'] += 1
            return False
    
    def ingest_batch(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Ingest multiple records in batch.
        
        Returns: Statistics dictionary
        """
        for record in records:
            self.ingest_record(record)
        
        # Save metadata after batch
        self.metadata_store.save()
        
        return self.get_statistics()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline statistics.
        """
        metadata_stats = self.metadata_store.get_statistics()
        placement_summary = self.placement_heuristics.get_placement_summary()
        
        return {
            'pipeline': self.stats,
            'metadata': metadata_stats,
            'placement': placement_summary,
            'sql_record_count': self.sql_manager.get_record_count(),
            'mongodb_record_count': self.mongo_manager.get_record_count()
        }
    
    def close(self):
        """
        Cleanup: Save metadata and close database connections.
        """
        print("[Pipeline] Shutting down...")
        self.metadata_store.save()
        self.sql_manager.close()
        self.mongo_manager.close()
        print("[Pipeline] Shutdown complete")


# Example usage
if __name__ == "__main__":
    # Create pipeline
    pipeline = IngestionPipeline()
    
    # Test records
    test_records = [
        {
            "username": "user1",
            "email": "user1@example.com",
            "age": 25,
            "ip_address": "192.168.1.1"
        },
        {
            "userName": "user2",
            "Email": "user2@example.com",
            "Age": 30,
            "IP": "10.0.0.1",
            "metadata": {
                "nested": {
                    "value": 123
                }
            }
        },
        {
            "username": "user3",
            "email": "user3@example.com",
            "device_id": "550e8400-e29b-41d4-a716-446655440000"
        }
    ]
    
    # Ingest batch
    print("Ingesting test records...")
    stats = pipeline.ingest_batch(test_records)
    
    # Print statistics
    print("\nPipeline Statistics:")
    print(json.dumps(stats, indent=2))
    
    # Cleanup
    pipeline.close()
