"""
Database Viewer - View and analyze SQL and MongoDB databases
Shows field placement decisions and database contents
"""
import sqlite3
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure


class DatabaseViewer:
    """View and analyze both SQL and MongoDB databases"""
    
    def __init__(self, sql_db='ingestion_data.db', 
                 mongo_uri='mongodb://localhost:27017/', 
                 mongo_db='ingestion_db',
                 metadata_file='metadata_store.json'):
        self.sql_db = sql_db
        self.mongo_uri = mongo_uri
        self.mongo_db_name = mongo_db
        self.metadata_file = metadata_file
        
        # Load metadata
        try:
            with open(metadata_file, 'r') as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            print(f"⚠️  Metadata file not found: {metadata_file}")
            self.metadata = {}

    def _safe_frequency(self, appearances):
        total = self.metadata.get('total_records', 0)
        if total <= 0:
            return 0.0
        return (appearances / total) * 100.0

    @staticmethod
    def _short_text(value, limit=80):
        text = str(value)
        if len(text) <= limit:
            return text
        return text[: limit - 3] + "..."
    
    def show_field_placements(self):
        """Show which fields went to SQL vs MongoDB"""
        print("\n" + "=" * 80)
        print("FIELD PLACEMENT DECISIONS")
        print("=" * 80)
        
        if not self.metadata.get('placement_decisions'):
            print("No placement decisions found. Run the ingestion system first.")
            return
        
        sql_fields = []
        mongo_fields = []
        both_fields = []
        
        for field, decision in self.metadata['placement_decisions'].items():
            backend = decision['backend']
            if backend == 'SQL':
                sql_fields.append(field)
            elif backend == 'MongoDB':
                mongo_fields.append(field)
            elif backend == 'Both':
                both_fields.append(field)
        
        print(f"\n📊 Summary:")
        print(f"  - SQL only: {len(sql_fields)} fields")
        print(f"  - MongoDB only: {len(mongo_fields)} fields")
        print(f"  - Both databases: {len(both_fields)} fields")
        print(f"  - Total unique fields: {len(self.metadata['placement_decisions'])}")
        
        if both_fields:
            print(f"\n✅ Fields in BOTH databases (for joins):")
            for field in both_fields:
                print(f"   - {field}")
        
        if sql_fields:
            print(f"\n🗄️  Fields in SQL only:")
            for field in sorted(sql_fields)[:20]:
                decision = self.metadata['placement_decisions'][field]
                field_stats = self.metadata['fields'].get(field, {})
                frequency = self._safe_frequency(field_stats.get('appearances', 0))
                print(f"   - {field:25} (freq: {frequency:5.1f}%) | {decision['reason'][:60]}...")
            if len(sql_fields) > 20:
                print(f"   ... and {len(sql_fields) - 20} more")
        
        if mongo_fields:
            print(f"\n🍃 Fields in MongoDB only:")
            for field in sorted(mongo_fields)[:20]:
                decision = self.metadata['placement_decisions'][field]
                field_stats = self.metadata['fields'].get(field, {})
                frequency = self._safe_frequency(field_stats.get('appearances', 0))
                print(f"   - {field:25} (freq: {frequency:5.1f}%) | {decision['reason'][:60]}...")
            if len(mongo_fields) > 20:
                print(f"   ... and {len(mongo_fields) - 20} more")

    def show_normalization_summary(self, limit=15):
        """Show SQL normalization metadata and table health."""
        print("\n" + "=" * 80)
        print("SQL NORMALIZATION SUMMARY")
        print("=" * 80)

        normalization = self.metadata.get('normalization', {})
        root_table = normalization.get('root_table', 'ingested_records')
        child_tables = normalization.get('child_tables', {})

        print(f"\nRoot table: {root_table}")
        print(f"Child tables registered: {len(child_tables)}")

        if not child_tables:
            print("No normalized child tables found in metadata.")
            return

        try:
            conn = sqlite3.connect(self.sql_db)
            cursor = conn.cursor()
            print("\n📋 Child table details:")

            shown = 0
            for table_name in sorted(child_tables.keys()):
                entry = child_tables[table_name]
                entity_path = entry.get('entity_path', 'unknown')
                columns = entry.get('columns', [])

                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                except sqlite3.OperationalError:
                    row_count = 'missing'

                print(
                    f"   - {table_name:30} rows={row_count:>8} "
                    f"entity={self._short_text(entity_path, 24)} "
                    f"columns={len(columns)}"
                )
                shown += 1
                if shown >= limit:
                    break

            remaining = len(child_tables) - shown
            if remaining > 0:
                print(f"   ... and {remaining} more child tables")

            conn.close()
        except Exception as error:
            print(f"❌ Could not query SQL normalization tables: {error}")

    def show_mongo_strategy(self, limit=20):
        """Show Mongo embed/reference decisions with telemetry."""
        print("\n" + "=" * 80)
        print("MONGODB STRATEGY SUMMARY")
        print("=" * 80)

        strategy = self.metadata.get('mongo_strategy', {})
        root_collection = strategy.get('root_collection', 'ingested_records')
        entities = strategy.get('entities', {})

        print(f"\nRoot collection: {root_collection}")
        print(f"Tracked entities: {len(entities)}")

        if not entities:
            print("No Mongo strategy entities found in metadata.")
            return

        embed_count = sum(1 for e in entities.values() if e.get('mode') == 'embed')
        reference_count = sum(1 for e in entities.values() if e.get('mode') == 'reference')
        print(f"Embed decisions: {embed_count}")
        print(f"Reference decisions: {reference_count}")

        print("\n📋 Entity decision details:")
        shown = 0
        for entity_path in sorted(entities.keys()):
            entry = entities[entity_path]
            mode = entry.get('mode', 'unknown')
            collection = entry.get('collection', 'n/a')
            score = entry.get('decision_score')
            threshold = entry.get('reference_threshold')
            reasons = entry.get('decision_reasons', [])

            telemetry = ""
            if score is not None and threshold is not None:
                telemetry = f" score={score}/{threshold}"
            if reasons:
                telemetry += f" reasons={self._short_text(','.join(reasons), 48)}"

            print(
                f"   - {entity_path:25} mode={mode:9} "
                f"collection={self._short_text(collection, 28)}{telemetry}"
            )

            shown += 1
            if shown >= limit:
                break

        remaining = len(entities) - shown
        if remaining > 0:
            print(f"   ... and {remaining} more entities")

    def show_buffer_status(self, limit=10):
        """Show metadata buffer state and Mongo buffer_records collection health."""
        print("\n" + "=" * 80)
        print("BUFFER PIPELINE STATUS")
        print("=" * 80)

        buffer_meta = self.metadata.get('buffer', {}).get('fields', {})
        print(f"\nTracked buffer fields in metadata: {len(buffer_meta)}")
        if buffer_meta:
            unresolved = [name for name, info in buffer_meta.items() if not info.get('resolved', False)]
            resolved = [name for name, info in buffer_meta.items() if info.get('resolved', False)]
            print(f"Resolved fields: {len(resolved)}")
            print(f"Unresolved fields: {len(unresolved)}")

            if unresolved:
                print("\n🕒 Top unresolved fields:")
                ranked = sorted(
                    unresolved,
                    key=lambda field: buffer_meta[field].get('observations', 0),
                    reverse=True,
                )
                for field in ranked[:limit]:
                    info = buffer_meta[field]
                    print(
                        f"   - {field:25} observations={info.get('observations', 0):5} "
                        f"first_seen={self._short_text(info.get('first_seen', 'n/a'), 28)}"
                    )

        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client[self.mongo_db_name]
            buffer_collection = db['buffer_records']
            doc_count = buffer_collection.count_documents({})
            print(f"\nMongoDB buffer_records documents: {doc_count:,}")

            if doc_count > 0:
                print("\n📝 Sample buffered documents:")
                docs = buffer_collection.find().limit(limit)
                for i, doc in enumerate(docs, 1):
                    field_keys = sorted((doc.get('fields') or {}).keys())
                    print(f"   {i:2}. user={doc.get('username', 'n/a'):20} fields={field_keys}")

                print(
                    "\nNote: historical rows may exist from earlier runs before field resolution. "
                    "Use the clean command or run a fresh ingestion cycle to verify current behavior."
                )

            client.close()
        except ConnectionFailure:
            print("\n❌ Cannot connect to MongoDB to inspect buffer_records")
        except Exception as error:
            print(f"\n❌ Unexpected MongoDB error while reading buffer status: {error}")
    
    def show_sql_data(self, limit=10):
        """Show SQL database contents"""
        print("\n" + "=" * 80)
        print(f"SQL DATABASE: {self.sql_db}")
        print("=" * 80)
        
        try:
            conn = sqlite3.connect(self.sql_db)
            cursor = conn.cursor()
            
            # Get schema
            cursor.execute("PRAGMA table_info(ingested_records)")
            columns = cursor.fetchall()
            
            print(f"\n📋 Schema ({len(columns)} columns):")
            for col in columns:
                col_id, name, col_type, not_null, default, pk = col
                constraints = []
                if pk:
                    constraints.append("PRIMARY KEY")
                if not_null:
                    constraints.append("NOT NULL")
                constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
                print(f"   {name:25} {col_type:15} {constraint_str}")
            
            # Get indexes
            cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='ingested_records'")
            indexes = cursor.fetchall()
            if indexes:
                print(f"\n🔑 Indexes:")
                for idx in indexes:
                    print(f"   - {idx[0]}")
            
            # Get record count
            cursor.execute("SELECT COUNT(*) FROM ingested_records")
            count = cursor.fetchone()[0]
            print(f"\n📊 Total records: {count:,}")
            
            # Show sample records
            if count > 0:
                print(f"\n📝 Sample records (first {min(limit, count)}):")
                cursor.execute(f"SELECT * FROM ingested_records LIMIT {limit}")
                rows = cursor.fetchall()
                col_names = [col[1] for col in columns]
                
                # Show in a formatted way
                for i, row in enumerate(rows, 1):
                    print(f"\n  Record {i}:")
                    for col_name, value in zip(col_names, row):
                        if value is not None:
                            value_str = str(value)[:50]
                            print(f"    {col_name:20} = {value_str}")
            
            conn.close()
            
        except sqlite3.OperationalError as e:
            print(f"❌ Error accessing SQL database: {e}")
            print(f"   File: {self.sql_db}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
    
    def show_mongodb_data(self, limit=10):
        """Show MongoDB database contents"""
        print("\n" + "=" * 80)
        print(f"MONGODB DATABASE: {self.mongo_db_name}")
        print("=" * 80)
        
        try:
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            
            db = client[self.mongo_db_name]
            collection = db['ingested_records']

            print("\n📚 Collections in database:")
            collection_names = sorted(db.list_collection_names())
            for name in collection_names:
                c = db[name].count_documents({})
                print(f"   - {name:25} docs={c}")
            
            # Get count
            count = collection.count_documents({})
            print(f"\n📊 Total documents: {count:,}")
            
            # Get field names from a sample document
            if count > 0:
                sample_doc = collection.find_one()
                field_names = list(sample_doc.keys())
                print(f"\n📋 Fields in collection ({len(field_names)}):")
                for field in sorted(field_names):
                    print(f"   - {field}")
                
                # Get indexes
                indexes = list(collection.list_indexes())
                if indexes:
                    print(f"\n🔑 Indexes:")
                    for idx in indexes:
                        print(f"   - {idx['name']}: {idx.get('key', {})}")
                
                # Show sample documents
                print(f"\n📝 Sample documents (first {min(limit, count)}):")
                docs = collection.find().limit(limit)
                for i, doc in enumerate(docs, 1):
                    print(f"\n  Document {i}:")
                    print(f"    _id: {doc['_id']}")
                    print(f"    username: {doc.get('username', 'N/A')}")
                    print(f"    sys_ingested_at: {doc.get('sys_ingested_at', 'N/A')}")
                    
                    # Show other fields (excluding large ones)
                    other_fields = {k: v for k, v in doc.items() 
                                  if k not in ['_id', 'username', 'sys_ingested_at', 't_stamp'] 
                                  and not isinstance(v, (dict, list))}
                    if other_fields:
                        print(f"    Other fields:")
                        for k, v in list(other_fields.items())[:10]:
                            print(f"      {k:20} = {v}")
                    
                    # Show nested structures
                    nested = {k: v for k, v in doc.items() if isinstance(v, (dict, list))}
                    if nested:
                        print(f"    Nested/Array fields:")
                        for k, v in nested.items():
                            print(f"      {k:20} = {json.dumps(v, default=str)[:80]}...")

            buffer_count = db['buffer_records'].count_documents({})
            if buffer_count > 0:
                print(f"\n⚠ buffer_records currently has {buffer_count} documents")
                sample_buffer_docs = db['buffer_records'].find().limit(min(limit, 5))
                for i, doc in enumerate(sample_buffer_docs, 1):
                    print(
                        f"   buffer_doc_{i}: user={doc.get('username', 'n/a')} "
                        f"fields={sorted((doc.get('fields') or {}).keys())}"
                    )
            
            client.close()
            
        except ConnectionFailure:
            print("❌ Cannot connect to MongoDB")
            print("   Make sure MongoDB is running: Start-Service MongoDB")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
    
    def show_statistics(self):
        """Show overall statistics"""
        print("\n" + "=" * 80)
        print("SYSTEM STATISTICS")
        print("=" * 80)
        
        if not self.metadata:
            print("No metadata available.")
            return
        
        print(f"\n📈 Ingestion Stats:")
        print(f"   Total records processed: {self.metadata.get('total_records', 0):,}")
        print(f"   Unique fields discovered: {len(self.metadata.get('fields', {}))}")
        print(f"   Placement decisions: {len(self.metadata.get('placement_decisions', {}))}")
        print(f"   Buffer fields tracked: {len(self.metadata.get('buffer', {}).get('fields', {}))}")
        print(f"   Normalized child tables: {len(self.metadata.get('normalization', {}).get('child_tables', {}))}")
        print(f"   Mongo strategy entities: {len(self.metadata.get('mongo_strategy', {}).get('entities', {}))}")
        
        print(f"\n⏰ Timeline:")
        print(f"   Session started: {self.metadata.get('session_start', 'N/A')}")
        print(f"   Last updated: {self.metadata.get('last_updated', 'N/A')}")
    
    def search_field(self, field_name):
        """Search for a specific field and show its placement"""
        print("\n" + "=" * 80)
        print(f"FIELD SEARCH: {field_name}")
        print("=" * 80)
        
        field_data = self.metadata.get('fields', {}).get(field_name)
        if not field_data:
            print(f"Field '{field_name}' not found in metadata.")
            print("\nAvailable fields:")
            for f in sorted(self.metadata.get('fields', {}).keys())[:20]:
                print(f"  - {f}")
            return
        
        placement = self.metadata.get('placement_decisions', {}).get(field_name, {})
        mapping = self.metadata.get('field_mappings', {}).get(field_name, {})
        mongo_entity = self.metadata.get('mongo_strategy', {}).get('entities', {}).get(field_name, {})
        
        print(f"\n📍 Placement: {placement.get('backend', 'Unknown')}")
        print(f"   Reason: {placement.get('reason', 'N/A')}")
        if mapping:
            print(f"\n🧭 Field mapping:")
            print(f"   backend: {mapping.get('backend')} | status: {mapping.get('status')}" )
            print(f"   sql_table: {mapping.get('sql_table')} | mongo_collection: {mapping.get('mongo_collection')}")
        if mongo_entity:
            print("\n🍃 Mongo strategy telemetry:")
            print(f"   mode: {mongo_entity.get('mode')} | collection: {mongo_entity.get('collection')}")
            print(
                f"   decision_score: {mongo_entity.get('decision_score')} / "
                f"{mongo_entity.get('reference_threshold')}"
            )
            reasons = mongo_entity.get('decision_reasons', [])
            if reasons:
                print(f"   decision_reasons: {', '.join(reasons)}")
        
        print(f"\n📊 Statistics:")
        print(f"   Appearances: {field_data.get('appearances', 0)}")
        frequency = self._safe_frequency(field_data.get('appearances', 0))
        print(f"   Frequency: {frequency:.1f}%")
        
        print(f"\n🔤 Types observed:")
        for  type_name, count in field_data.get('type_counts', {}).items():
            percentage = (count / field_data.get('appearances', 1)) * 100
            print(f"   - {type_name:15} {count:5} times ({percentage:5.1f}%)")
        
        print(f"\n💾 Sample values:")
        for value in field_data.get('sample_values', [])[:5]:
            print(f"   - {value}")


def main():
    import sys
    
    print("\n" + "🔍 " * 40)
    print("\n  DATABASE VIEWER - Autonomous Data Ingestion System")
    print("\n" + "🔍 " * 40)
    
    viewer = DatabaseViewer()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'placements':
            viewer.show_field_placements()
        elif command == 'normalization':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 15
            viewer.show_normalization_summary(limit)
        elif command == 'mongo_strategy':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
            viewer.show_mongo_strategy(limit)
        elif command == 'buffer':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            viewer.show_buffer_status(limit)
        elif command == 'sql':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            viewer.show_sql_data(limit)
        elif command == 'mongodb':
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            viewer.show_mongodb_data(limit)
        elif command == 'stats':
            viewer.show_statistics()
        elif command == 'search':
            if len(sys.argv) > 2:
                viewer.search_field(sys.argv[2])
            else:
                print("Usage: python view_databases.py search <field_name>")
        elif command == 'all':
            viewer.show_statistics()
            viewer.show_field_placements()
            viewer.show_normalization_summary(10)
            viewer.show_mongo_strategy(10)
            viewer.show_buffer_status(5)
            viewer.show_sql_data(5)
            viewer.show_mongodb_data(5)
        else:
            print(f"Unknown command: {command}")
            print_help()
    else:
        # Default: show everything
        viewer.show_statistics()
        viewer.show_field_placements()
        viewer.show_normalization_summary(10)
        viewer.show_mongo_strategy(10)
        viewer.show_buffer_status(5)
        viewer.show_sql_data(5)
        viewer.show_mongodb_data(5)


def print_help():
    print("""
Usage: python view_databases.py [command] [args]

Commands:
  placements          Show which fields went to SQL vs MongoDB
    normalization [n]   Show normalized SQL child tables (default: 15)
    mongo_strategy [n]  Show embed/reference decisions + telemetry (default: 20)
    buffer [n]          Show buffer field and buffer_records status (default: 10)
  sql [limit]         Show SQL database contents (default: 10 records)
  mongodb [limit]     Show MongoDB database contents (default: 10 records)
  stats               Show system statistics
  search <field>      Search for a specific field
  all                 Show everything (default)

Examples:
  python view_databases.py
  python view_databases.py placements
    python view_databases.py normalization
    python view_databases.py mongo_strategy 25
    python view_databases.py buffer 5
  python view_databases.py sql 20
  python view_databases.py mongodb 5
  python view_databases.py search email
  python view_databases.py search metadata_sensor_data_version
    """)


if __name__ == "__main__":
    main()
