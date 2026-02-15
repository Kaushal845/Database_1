"""
Database Viewer - View and analyze SQL and MongoDB databases
Shows field placement decisions and database contents
"""
import sqlite3
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime


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
                frequency = (field_stats.get('appearances', 0) / self.metadata.get('total_records', 1)) * 100
                print(f"   - {field:25} (freq: {frequency:5.1f}%) | {decision['reason'][:60]}...")
            if len(sql_fields) > 20:
                print(f"   ... and {len(sql_fields) - 20} more")
        
        if mongo_fields:
            print(f"\n🍃 Fields in MongoDB only:")
            for field in sorted(mongo_fields)[:20]:
                decision = self.metadata['placement_decisions'][field]
                field_stats = self.metadata['fields'].get(field, {})
                frequency = (field_stats.get('appearances', 0) / self.metadata.get('total_records', 1)) * 100
                print(f"   - {field:25} (freq: {frequency:5.1f}%) | {decision['reason'][:60]}...")
            if len(mongo_fields) > 20:
                print(f"   ... and {len(mongo_fields) - 20} more")
    
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
        
        print(f"\n📍 Placement: {placement.get('backend', 'Unknown')}")
        print(f"   Reason: {placement.get('reason', 'N/A')}")
        
        print(f"\n📊 Statistics:")
        print(f"   Appearances: {field_data.get('appearances', 0)}")
        frequency = (field_data.get('appearances', 0) / self.metadata.get('total_records', 1)) * 100
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
            viewer.show_sql_data(5)
            viewer.show_mongodb_data(5)
        else:
            print(f"Unknown command: {command}")
            print_help()
    else:
        # Default: show everything
        viewer.show_statistics()
        viewer.show_field_placements()
        viewer.show_sql_data(5)
        viewer.show_mongodb_data(5)


def print_help():
    print("""
Usage: python view_databases.py [command] [args]

Commands:
  placements          Show which fields went to SQL vs MongoDB
  sql [limit]         Show SQL database contents (default: 10 records)
  mongodb [limit]     Show MongoDB database contents (default: 10 records)
  stats               Show system statistics
  search <field>      Search for a specific field
  all                 Show everything (default)

Examples:
  python view_databases.py
  python view_databases.py placements
  python view_databases.py sql 20
  python view_databases.py mongodb 5
  python view_databases.py search email
  python view_databases.py search metadata_sensor_data_version
    """)


if __name__ == "__main__":
    main()
