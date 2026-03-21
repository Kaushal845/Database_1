"""
Data Ingestion Pipeline - Assignment-2 hybrid framework orchestrator.
"""
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone
import json

from metadata_store import MetadataStore
from type_detector import TypeDetector
from placement_heuristics import PlacementHeuristics
from database_managers import SQLManager, MongoDBManager
from query_engine import MetadataDrivenQueryEngine


class IngestionPipeline:
    """
    Assignment-2 autonomous hybrid ingestion system:
    1. Registers schema versions
    2. Tracks field statistics and metadata
    3. Routes fields through SQL / MongoDB / Buffer
    4. Normalizes repeating entities into SQL child tables
    5. Applies MongoDB embed/reference strategy
    6. Exposes metadata-driven CRUD operations
    """
    
    def __init__(self, 
                 metadata_file='metadata_store.json',
                 sql_db='ingestion_data.db',
                 mongo_uri='mongodb://localhost:27017/',
                 mongo_db='ingestion_db'):
        
        # Initialize components
        self.metadata_store = MetadataStore(metadata_file)
        self.type_detector = TypeDetector()
        self.placement_heuristics = PlacementHeuristics(self.metadata_store)
        
        # Initialize databases
        self.sql_manager = SQLManager(sql_db)
        self.mongo_manager = MongoDBManager(mongo_uri, mongo_db)
        self.query_engine = MetadataDrivenQueryEngine(
            metadata_store=self.metadata_store,
            sql_manager=self.sql_manager,
            mongo_manager=self.mongo_manager,
            ingest_callback=self.ingest_record,
        )
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'sql_inserts': 0,
            'mongodb_inserts': 0,
            'buffer_inserts': 0,
            'errors': 0,
            'fields_discovered': 0
        }
        
        print("[Pipeline] Initialized successfully")
        print(f"[Pipeline] Loaded {len(self.metadata_store.metadata['fields'])} known fields")

    def register_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Register user-provided JSON schema for ingestion constraints."""
        version = self.metadata_store.register_schema(schema)
        return {
            'schema_version': version,
            'registered_at': datetime.now(timezone.utc).isoformat(),
            'schema_keys': list(schema.keys()),
        }
    
    def _iter_field_paths(self, value: Any, prefix: str = '') -> List[Tuple[str, Any]]:
        """Yield field paths recursively for metadata tracking."""
        pairs: List[Tuple[str, Any]] = []
        if isinstance(value, dict):
            for key, child in value.items():
                path = f"{prefix}.{key}" if prefix else key
                pairs.append((path, child))
                pairs.extend(self._iter_field_paths(child, path))
            return pairs

        if isinstance(value, list):
            pairs.append((prefix, value))
            for index, child in enumerate(value):
                child_path = f"{prefix}[{index}]"
                pairs.extend(self._iter_field_paths(child, child_path))
            return pairs

        if prefix:
            pairs.append((prefix, value))
        return pairs

    @staticmethod
    def _is_scalar(value: Any) -> bool:
        return not isinstance(value, (dict, list))

    def _extract_repeating_entities(self, record: Dict[str, Any], prefix: str = '') -> Dict[str, List[Dict[str, Any]]]:
        """Detect repeating groups represented as arrays of objects."""
        entities: Dict[str, List[Dict[str, Any]]] = {}
        for key, value in record.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
                entities[path] = value
            elif isinstance(value, dict):
                entities.update(self._extract_repeating_entities(value, path))
        return entities

    def _decide_mongo_mode(self, entity_path: str, value: Any) -> Tuple[str, str]:
        """Decide whether nested data should be embedded or referenced."""
        if isinstance(value, list):
            if len(value) > 3 and any(isinstance(item, dict) for item in value):
                collection = f"ref_{entity_path.replace('.', '_').replace('[', '_').replace(']', '')}"
                self.metadata_store.register_mongo_entity(entity_path, 'reference', collection)
                return ('reference', collection)

            self.metadata_store.register_mongo_entity(entity_path, 'embed', 'ingested_records')
            return ('embed', 'ingested_records')

        if isinstance(value, dict):
            if len(value.keys()) > 6:
                collection = f"ref_{entity_path.replace('.', '_')}"
                self.metadata_store.register_mongo_entity(entity_path, 'reference', collection)
                return ('reference', collection)

            self.metadata_store.register_mongo_entity(entity_path, 'embed', 'ingested_records')
            return ('embed', 'ingested_records')

        self.metadata_store.register_mongo_entity(entity_path, 'embed', 'ingested_records')
        return ('embed', 'ingested_records')
    
    def _track_stats(self, record: Dict[str, Any]):
        """Track semantic type stats for all recursive field paths."""
        for path, value in self._iter_field_paths(record):
            if not path:
                continue
            detected_type = self.type_detector.detect_type(value)
            self.metadata_store.update_field_stats(path, detected_type, value)
    
    def _add_temporal_timestamps(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add bi-temporal timestamps:
        1. t_stamp: Client timestamp (from JSON or current time if missing)
        2. sys_ingested_at: Server timestamp (unique, generated here)
        """
        # Server timestamp (unique identifier for this record)
        sys_timestamp = datetime.now(timezone.utc).isoformat() + f".{self.stats['total_processed']:08d}"
        record['sys_ingested_at'] = sys_timestamp
        
        # Client timestamp (preserve if exists, otherwise use current)
        if 't_stamp' not in record and 'timestamp' not in record:
            record['t_stamp'] = datetime.now(timezone.utc).isoformat()
        elif 'timestamp' in record and 't_stamp' not in record:
            record['t_stamp'] = record['timestamp']
        
        return record

    def _route_scalar_field(
        self,
        field_name: str,
        value: Any,
        sql_record: Dict[str, Any],
        mongo_record: Dict[str, Any],
        unresolved_fields: Dict[str, Any],
    ):
        """Route a scalar top-level field to SQL, MongoDB, Both, or Buffer."""
        previous_mapping = self.metadata_store.get_field_mapping(field_name)
        placement = self.placement_heuristics.decide_placement(field_name)

        if placement == 'Buffer':
            unresolved_fields[field_name] = value
            self.metadata_store.add_buffer_observation(field_name, value)
            self.metadata_store.set_field_mapping(field_name, 'Buffer', status='buffer')
            return

        if placement in ('SQL', 'Both'):
            detected_type = self.type_detector.detect_type(value)
            is_unique = self.placement_heuristics.should_be_unique(field_name)
            self.sql_manager.add_column_if_not_exists(field_name, detected_type, is_unique)
            sql_record[field_name] = value

        if placement in ('MongoDB', 'Both'):
            mongo_record[field_name] = value

        if previous_mapping and previous_mapping.get('status') == 'buffer':
            self._drain_buffered_field(
                field_name=field_name,
                final_backend=placement,
                mongo_mode='embed',
                mongo_collection='ingested_records',
            )

        self.metadata_store.set_field_mapping(
            field_name,
            placement,
            sql_table='ingested_records' if placement in ('SQL', 'Both') else None,
            mongo_collection='ingested_records' if placement in ('MongoDB', 'Both') else None,
            status='final',
        )

    def _drain_buffered_field(
        self,
        field_name: str,
        final_backend: str,
        mongo_mode: str = 'embed',
        mongo_collection: str = 'ingested_records',
    ) -> int:
        """Migrate previously buffered values to their resolved backend."""
        migrated_count = 0
        buffer_docs = self.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name='buffer_records',
            limit=1_000_000,
        )

        for doc in buffer_docs:
            buffered = doc.get('fields', {})
            if not isinstance(buffered, dict) or field_name not in buffered:
                continue

            sys_ingested_at = doc.get('sys_ingested_at')
            if not sys_ingested_at:
                continue

            value = buffered[field_name]

            if final_backend in ('SQL', 'Both'):
                detected_type = self.type_detector.detect_type(value)
                is_unique = self.placement_heuristics.should_be_unique(field_name)
                self.sql_manager.add_column_if_not_exists(field_name, detected_type, is_unique)
                self.sql_manager.update_root_field(sys_ingested_at, field_name, value)

            if final_backend in ('MongoDB', 'Both'):
                if mongo_mode == 'embed':
                    self.mongo_manager.update_root_field(sys_ingested_at, field_name, value)
                else:
                    if isinstance(value, list):
                        for index, item in enumerate(value):
                            self.mongo_manager.insert_record(
                                {
                                    'parent_sys_ingested_at': sys_ingested_at,
                                    'entity_path': field_name,
                                    'item_index': index,
                                    'payload': item,
                                },
                                collection_name=mongo_collection,
                            )
                    else:
                        self.mongo_manager.insert_record(
                            {
                                'parent_sys_ingested_at': sys_ingested_at,
                                'entity_path': field_name,
                                'payload': value,
                            },
                            collection_name=mongo_collection,
                        )

                    self.mongo_manager.update_root_field(
                        sys_ingested_at,
                        f"{field_name}_ref_collection",
                        mongo_collection,
                    )

            if self.mongo_manager.remove_buffer_field(sys_ingested_at, field_name, 'buffer_records'):
                migrated_count += 1

        return migrated_count

    def _normalize_entities(
        self,
        enriched_record: Dict[str, Any],
        repeating_entities: Dict[str, List[Dict[str, Any]]],
    ):
        """Create SQL child tables for repeating entities and insert rows."""
        parent_key = enriched_record['sys_ingested_at']

        for entity_path, rows in repeating_entities.items():
            table_name = f"norm_{entity_path.replace('.', '_').replace('[', '_').replace(']', '')}"

            column_types: Dict[str, str] = {}
            for row in rows:
                for key, value in row.items():
                    if not self._is_scalar(value):
                        continue
                    column_types[key] = self.type_detector.detect_type(value)

            if not column_types:
                continue

            self.sql_manager.ensure_child_table(table_name, column_types)

            scalar_rows = []
            for row in rows:
                scalar_rows.append({k: v for k, v in row.items() if self._is_scalar(v)})
            self.sql_manager.insert_child_rows(table_name, parent_key, scalar_rows)

            self.metadata_store.register_normalized_table(
                table_name=table_name,
                entity_path=entity_path,
                columns=list(column_types.keys()),
            )
            self.metadata_store.set_field_mapping(
                entity_path,
                backend='Both',
                sql_table=table_name,
                mongo_collection=f"ref_{entity_path.replace('.', '_')}",
                status='final',
            )

    def _apply_mongo_document_strategy(
        self,
        enriched_record: Dict[str, Any],
        mongo_record: Dict[str, Any],
        top_level_fields: Dict[str, Any],
        unresolved_fields: Dict[str, Any],
    ):
        """Apply embed/reference decisions and write referenced entities to MongoDB."""
        for field_name, value in top_level_fields.items():
            if self._is_scalar(value):
                continue

            previous_mapping = self.metadata_store.get_field_mapping(field_name)
            mode, collection_name = self._decide_mongo_mode(field_name, value)
            placement = self.placement_heuristics.decide_placement(field_name)
            if placement == 'Buffer':
                unresolved_fields[field_name] = value
                self.metadata_store.add_buffer_observation(field_name, value)
                self.metadata_store.set_field_mapping(
                    field_name,
                    'Buffer',
                    sql_table=None,
                    mongo_collection=collection_name,
                    status='buffer',
                )
                continue

            if previous_mapping and previous_mapping.get('status') == 'buffer':
                self._drain_buffered_field(
                    field_name=field_name,
                    final_backend=placement,
                    mongo_mode=mode,
                    mongo_collection=collection_name,
                )

            if mode == 'embed':
                mongo_record[field_name] = value
            else:
                if isinstance(value, list):
                    for index, item in enumerate(value):
                        self.mongo_manager.insert_record(
                            {
                                'parent_sys_ingested_at': enriched_record['sys_ingested_at'],
                                'entity_path': field_name,
                                'item_index': index,
                                'payload': item,
                            },
                            collection_name=collection_name,
                        )
                else:
                    self.mongo_manager.insert_record(
                        {
                            'parent_sys_ingested_at': enriched_record['sys_ingested_at'],
                            'entity_path': field_name,
                            'payload': value,
                        },
                        collection_name=collection_name,
                    )

                mongo_record[f"{field_name}_ref_collection"] = collection_name

            self.metadata_store.set_field_mapping(
                field_name,
                backend='MongoDB',
                sql_table=None,
                mongo_collection=collection_name,
                status='final',
            )
    
    def ingest_record(self, raw_record: Dict[str, Any]) -> bool:
        """
        Ingest a single JSON record through the complete pipeline.
        
        Assignment-2 steps:
        1. Track field statistics across all paths
        2. Add bi-temporal timestamps
        3. Classify top-level scalar fields (SQL/MongoDB/Buffer/Both)
        4. Detect and normalize repeating entities for SQL
        5. Apply MongoDB embed/reference strategy for nested structures
        6. Persist buffer fields for deferred decisions
        
        Returns: True if successful, False otherwise
        """
        try:
            enriched_record = dict(raw_record)

            # Step 1: Track stats first (raw record)
            self._track_stats(enriched_record)

            # Step 2: Add timestamps and increment counters
            enriched_record = self._add_temporal_timestamps(enriched_record)
            self.metadata_store.increment_total_records()

            sql_record = {
                'username': enriched_record.get('username', 'unknown_user'),
                'sys_ingested_at': enriched_record['sys_ingested_at'],
                't_stamp': enriched_record['t_stamp'],
            }
            mongo_record = {
                'username': enriched_record.get('username', 'unknown_user'),
                'sys_ingested_at': enriched_record['sys_ingested_at'],
                't_stamp': enriched_record['t_stamp'],
            }
            unresolved_fields: Dict[str, Any] = {}

            # Step 3: Scalar field routing
            for field_name, value in enriched_record.items():
                if field_name in ('username', 'sys_ingested_at', 't_stamp'):
                    continue
                if self._is_scalar(value):
                    self._route_scalar_field(field_name, value, sql_record, mongo_record, unresolved_fields)

            # Step 4: Detect repeating entities for SQL normalization
            repeating_entities = self._extract_repeating_entities(enriched_record)

            # Step 5: Mongo document strategy for nested content
            self._apply_mongo_document_strategy(
                enriched_record,
                mongo_record,
                enriched_record,
                unresolved_fields,
            )

            # Step 6: Insert primary records
            sql_success = False
            mongo_success = False
            
            if sql_record and len(sql_record) > 0:
                sql_success = self.sql_manager.insert_record(sql_record)
                if sql_success:
                    self.stats['sql_inserts'] += 1
                    # Normalize child entities only after parent row exists.
                    self._normalize_entities(enriched_record, repeating_entities)
            
            if mongo_record and len(mongo_record) > 0:
                mongo_success = self.mongo_manager.insert_record(mongo_record)
                if mongo_success:
                    self.stats['mongodb_inserts'] += 1

            if unresolved_fields:
                buffer_payload = {
                    'username': enriched_record.get('username', 'unknown_user'),
                    'sys_ingested_at': enriched_record['sys_ingested_at'],
                    'fields': unresolved_fields,
                }
                buffer_success = self.mongo_manager.insert_record(buffer_payload, collection_name='buffer_records')
                if buffer_success:
                    self.stats['buffer_inserts'] += 1
            
            # Update statistics
            self.stats['total_processed'] += 1
            
            # Periodic metadata save (every 10 records)
            if self.stats['total_processed'] % 10 == 0:
                self.metadata_store.save()
            
            # Log progress every 50 records
            if self.stats['total_processed'] % 50 == 0:
                print(f"[Pipeline] Processed {self.stats['total_processed']} records "
                      f"(SQL: {self.stats['sql_inserts']}, MongoDB: {self.stats['mongodb_inserts']}, "
                      f"Buffer: {self.stats['buffer_inserts']})")
            
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

    def execute_crud(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute metadata-driven CRUD request."""
        return self.query_engine.execute(request)
    
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
            'mongodb_record_count': self.mongo_manager.get_record_count(),
            'buffer_record_count': self.mongo_manager.get_record_count('buffer_records'),
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
            "ip_address": "192.168.1.1",
            "orders": [
                {"item": "book", "qty": 1, "price": 12.5},
                {"item": "bag", "qty": 2, "price": 35.0}
            ]
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

    # Example metadata-driven read query
    read_result = pipeline.execute_crud(
        {
            'operation': 'read',
            'fields': ['username', 'email', 'orders'],
            'filters': {'username': 'user1'},
        }
    )
    print("\nCRUD Read Result:")
    print(json.dumps(read_result, indent=2))
    
    # Cleanup
    pipeline.close()

