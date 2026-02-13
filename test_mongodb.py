"""
MongoDB Connection Test Script
Checks if MongoDB is accessible from Python
"""
from pymongo import MongoClient
import pymongo
import sys


def test_mongodb_connection(uri='mongodb://localhost:27017/', timeout=5000):
    """Test MongoDB connection"""
    print("=" * 70)
    print("MongoDB Connection Test")
    print("=" * 70)
    print(f"\nTesting connection to: {uri}")
    print(f"Timeout: {timeout}ms")
    
    try:
        # Try to connect
        print("\n[1/5] Connecting to MongoDB...")
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout)
        
        # Ping the server
        print("[2/5] Pinging server...")
        client.admin.command('ping')
        print("✅ MongoDB is reachable!")
        
        # Get server info
        print("\n[3/5] Getting server information...")
        server_info = client.server_info()
        print(f"✅ MongoDB version: {server_info['version']}")
        print(f"✅ Platform: {server_info.get('os', {}).get('name', 'Unknown')}")
        
        # Test database operations
        print("\n[4/5] Testing database operations...")
        db = client['test_connection_db']
        collection = db['test_collection']
        
        # Insert test document
        result = collection.insert_one({"test": "data", "timestamp": "test"})
        print(f"✅ Insert successful! Document ID: {result.inserted_id}")
        
        # Read back
        doc = collection.find_one({"_id": result.inserted_id})
        print(f"✅ Query successful! Document: {doc}")
        
        # Cleanup
        print("\n[5/5] Cleaning up...")
        db.drop_collection('test_collection')
        print("✅ Cleanup complete!")
        
        client.close()
        
        print("\n" + "=" * 70)
        print("✅ SUCCESS: MongoDB is fully operational!")
        print("=" * 70)
        print("\nYou can now run the ingestion system:")
        print("  python data_consumer.py")
        
        return True
        
    except pymongo.errors.ServerSelectionTimeoutError:
        print("\n❌ FAILED: Cannot connect to MongoDB")
        print("\n" + "=" * 70)
        print("MongoDB is NOT running or not accessible")
        print("=" * 70)
        
        print("\n📋 Troubleshooting steps:")
        print("\n1. Check if MongoDB service is running:")
        print("   PowerShell: Get-Service MongoDB")
        print("   Or: Start-Service MongoDB")
        
        print("\n2. Check if port 27017 is listening:")
        print("   PowerShell: netstat -an | findstr 27017")
        
        print("\n3. If MongoDB is not installed:")
        print("   See: MONGODB_INSTALLATION_GUIDE.md")
        
        print("\n4. Alternative: Run without MongoDB")
        print("   The system works perfectly with SQL only!")
        print("   Just run: python data_consumer.py")
        
        return False
        
    except pymongo.errors.OperationFailure as e:
        print(f"\n❌ FAILED: Authentication or permission error")
        print(f"   Error: {e}")
        print("\nCheck your MongoDB credentials or permissions")
        return False
        
    except Exception as e:
        print(f"\n❌ FAILED: Unexpected error")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_system_requirements():
    """Check if required packages are available"""
    print("\nChecking system requirements...")
    
    try:
        import pymongo
        print(f"✅ pymongo version: {pymongo.__version__}")
        return True
    except ImportError:
        print("❌ pymongo not installed")
        print("\nInstall it with: pip install pymongo")
        return False


if __name__ == "__main__":
    print("\n" + "🔧 " * 35)
    
    # Check requirements first
    if not test_system_requirements():
        sys.exit(1)
    
    # Test MongoDB
    print()
    
    # Allow custom URI from command line
    uri = 'mongodb://localhost:27017/'
    if len(sys.argv) > 1:
        uri = sys.argv[1]
    
    success = test_mongodb_connection(uri)
    
    print("\n" + "🔧 " * 35)
    
    sys.exit(0 if success else 1)
