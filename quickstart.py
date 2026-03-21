"""
Quick Start Script - Easy setup and execution
"""
import sys
import subprocess
import time
import json
import shutil
from pathlib import Path


PROJECT_MONGO_DBS = ["ingestion_db", "assignment2_test_db"]


def check_dependencies():
    """Check if required packages are installed"""
    print("Checking dependencies...")
    
    required = ['fastapi', 'uvicorn', 'faker', 'sse_starlette', 'pymongo', 'requests']
    missing = []
    
    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Missing packages: {', '.join(missing)}")
        print("\nInstalling missing packages...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✓ Dependencies installed")
    else:
        print("✓ All dependencies satisfied")


def check_mongodb():
    """Check if MongoDB is available"""
    print("\nChecking MongoDB...")
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        client.close()
        print("✓ MongoDB is running")
        return True
    except Exception as e:
        print("⚠ MongoDB not available - will use SQL only")
        return False


def check_api_server():
    """Check if FastAPI server is running"""
    print("\nChecking FastAPI server...")
    try:
        import requests
        response = requests.get('http://127.0.0.1:8000', timeout=2)
        print("✓ FastAPI server is running")
        return True
    except Exception:
        print("❌ FastAPI server is not running")
        return False


def _clean_mongodb_databases(db_names, mongo_uri='mongodb://localhost:27017/'):
    """Drop project MongoDB databases and return removed/missing names."""
    removed = []
    missing = []

    try:
        from pymongo import MongoClient
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
    except Exception:
        return removed, missing, "MongoDB unavailable, skipped Mongo cleanup."

    try:
        existing = set(client.list_database_names())
        for db_name in db_names:
            if db_name in existing:
                client.drop_database(db_name)
                removed.append(db_name)
            else:
                missing.append(db_name)
    finally:
        client.close()

    return removed, missing, None


def clean_generated_files(force: bool = False, clean_mongo: bool = True):
    """Delete generated project artifacts and caches."""
    root = Path(__file__).resolve().parent
    removed = []
    missing = []

    file_targets = [
        root / "ingestion_data.db",
        root / "metadata_store.json",
    ]
    dir_targets = [
        root / ".pytest_cache",
        root / "__pycache__",
    ]

    for cache_dir in root.rglob("__pycache__"):
        if cache_dir not in dir_targets:
            dir_targets.append(cache_dir)

    if not force:
        print("This will delete generated local artifacts:")
        print("- ingestion_data.db")
        print("- metadata_store.json")
        print("- .pytest_cache/")
        print("- all __pycache__/ directories")
        if clean_mongo:
            print(f"- MongoDB databases: {', '.join(PROJECT_MONGO_DBS)}")
        confirmation = input("Proceed? [y/N]: ").strip().lower()
        if confirmation not in {"y", "yes"}:
            print("Cleanup cancelled.")
            return

    for path in file_targets:
        if path.exists():
            path.unlink()
            removed.append(str(path.relative_to(root)))
        else:
            missing.append(str(path.relative_to(root)))

    for path in dir_targets:
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            removed.append(str(path.relative_to(root)) + "/")

    for pyc_file in root.rglob("*.pyc"):
        pyc_file.unlink()
        removed.append(str(pyc_file.relative_to(root)))

    mongo_note = None
    mongo_removed = []
    mongo_missing = []
    if clean_mongo:
        mongo_removed, mongo_missing, mongo_note = _clean_mongodb_databases(PROJECT_MONGO_DBS)
        for db_name in mongo_removed:
            removed.append(f"MongoDB:{db_name}")
        for db_name in mongo_missing:
            missing.append(f"MongoDB:{db_name}")

    print("\nCleanup complete.")
    if removed:
        print("Removed:")
        for item in sorted(set(removed)):
            print(f"- {item}")
    if missing:
        print("Not found (already clean):")
        for item in sorted(set(missing)):
            print(f"- {item}")
    if mongo_note:
        print(f"- {mongo_note}")


def main():
    if len(sys.argv) > 1 and sys.argv[1].lower() in {"clean", "--clean"}:
        force = any(arg in {"-y", "--yes"} for arg in sys.argv[2:])
        clean_mongo = not any(arg in {"--skip-mongo", "--no-mongo"} for arg in sys.argv[2:])
        clean_generated_files(force=force, clean_mongo=clean_mongo)
        return

    print("=" * 70)
    print("AUTONOMOUS DATA INGESTION SYSTEM - QUICK START")
    print("=" * 70)
    
    # Step 1: Check dependencies
    check_dependencies()
    
    # Step 2: Check MongoDB
    has_mongo = check_mongodb()
    
    # Step 3: Check if API server is running
    api_running = check_api_server()

    schema = None
    if api_running:
        try:
            import requests
            schema_resp = requests.get('http://127.0.0.1:8000/schema', timeout=2)
            if schema_resp.status_code == 200:
                schema = schema_resp.json()
                print("✓ Generator schema fetched")
        except Exception:
            schema = None
    
    if not api_running:
        print("\n" + "=" * 70)
        print("SETUP REQUIRED")
        print("=" * 70)
        print("\nThe FastAPI server needs to be running.")
        print("\nPlease open a NEW terminal window and run:")
        print("  uvicorn main:app --reload --port 8000")
        print("\nThen press Enter here to continue...")
        input()
        
        # Check again
        api_running = check_api_server()
        if not api_running:
            print("\n❌ API server still not detected.")
            print("Please start it manually and run this script again.")
            sys.exit(1)
    
    # Step 4: Configuration
    print("\n" + "=" * 70)
    print("CONFIGURATION")
    print("=" * 70)
    
    print("\nHow many records would you like to ingest?")
    print("1. Small test (100 records)")
    print("2. Medium test (1,000 records)")
    print("3. Large test (10,000 records)")
    print("4. Custom")
    
    choice = input("\nChoice [1-4]: ").strip()
    
    if choice == '1':
        batch_size = 50
        total_batches = 2
    elif choice == '2':
        batch_size = 100
        total_batches = 10
    elif choice == '3':
        batch_size = 100
        total_batches = 100
    elif choice == '4':
        try:
            batch_size = int(input("Batch size: "))
            total_batches = int(input("Number of batches: "))
        except ValueError:
            print("Invalid input. Using defaults.")
            batch_size = 100
            total_batches = 10
    else:
        print("Invalid choice. Using defaults.")
        batch_size = 100
        total_batches = 10
    
    total_records = batch_size * total_batches
    
    print(f"\n✓ Configuration:")
    print(f"  - Batch size: {batch_size}")
    print(f"  - Total batches: {total_batches}")
    print(f"  - Total records: {total_records}")
    print(f"  - MongoDB: {'Enabled' if has_mongo else 'Disabled (SQL only)'}")
    
    # Step 5: Run ingestion
    print("\n" + "=" * 70)
    print("STARTING INGESTION")
    print("=" * 70)
    print("\nPress Ctrl+C at any time to stop.")
    print("\nStarting in 3 seconds...")
    time.sleep(3)
    
    try:
        # Import and run
        from data_consumer import DataConsumer
        
        consumer = DataConsumer(api_url='http://127.0.0.1:8000', schema=schema)
        consumer.consume_continuous(
            batch_size=batch_size,
            total_batches=total_batches,
            delay=0.5,
            close_on_finish=False,
        )

        # Step 6: Sample metadata-driven read query
        print("\n" + "=" * 70)
        print("RUNNING SAMPLE READ QUERY")
        print("=" * 70)
        read_response = consumer.execute_query(
            {
                "operation": "read",
                "fields": ["username", "email", "orders", "comments"],
                "limit": 3,
            }
        )
        print(json.dumps(read_response, indent=2)[:1200])
        
        print("\n" + "=" * 70)
        print("COMPLETE!")
        print("=" * 70)
        print("\n✓ Data ingestion complete")
        print(f"✓ Metadata saved: metadata_store.json")
        print(f"✓ SQL database: ingestion_data.db")
        if has_mongo:
            print(f"✓ MongoDB: ingestion_db collections")
        
        print("\nNext steps:")
        print("1. Read docs/ASSIGNMENT2_TECHNICAL_REPORT.md for detailed analysis")
        print("2. Query the databases:")
        print("   - SQLite: sqlite3 ingestion_data.db")
        if has_mongo:
            print("   - MongoDB: mongo ingestion_db")
        consumer.close()
        
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'consumer' in locals():
            consumer.close()


if __name__ == "__main__":
    main()
