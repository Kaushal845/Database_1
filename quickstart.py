"""
Quick Start Script - Easy setup and execution
"""
import sys
import subprocess
import time
import os


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


def main():
    print("=" * 70)
    print("AUTONOMOUS DATA INGESTION SYSTEM - QUICK START")
    print("=" * 70)
    
    # Step 1: Check dependencies
    check_dependencies()
    
    # Step 2: Check MongoDB
    has_mongo = check_mongodb()
    
    # Step 3: Check if API server is running
    api_running = check_api_server()
    
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
        
        consumer = DataConsumer(api_url='http://127.0.0.1:8000')
        consumer.consume_continuous(
            batch_size=batch_size,
            total_batches=total_batches,
            delay=0.5
        )
        
        # Step 6: Generate report
        print("\n" + "=" * 70)
        print("GENERATING REPORT")
        print("=" * 70)
        
        from report_generator import ReportGenerator
        from metadata_store import MetadataStore
        
        metadata_store = MetadataStore('metadata_store.json')
        generator = ReportGenerator(metadata_store)
        generator.generate_full_report()
        
        print("\n" + "=" * 70)
        print("COMPLETE!")
        print("=" * 70)
        print("\n✓ Data ingestion complete")
        print("✓ Technical report generated: TECHNICAL_REPORT.md")
        print(f"✓ Metadata saved: metadata_store.json")
        print(f"✓ SQL database: ingestion_data.db")
        if has_mongo:
            print(f"✓ MongoDB: ingestion_db.ingested_records")
        
        print("\nNext steps:")
        print("1. Read TECHNICAL_REPORT.md for detailed analysis")
        print("2. Query the databases:")
        print("   - SQLite: sqlite3 ingestion_data.db")
        if has_mongo:
            print("   - MongoDB: mongo ingestion_db")
        
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
