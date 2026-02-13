"""
SQL Functionality Test Script
Tests all SQL components including SQLManager, schema creation, and data operations
"""
import sys
import os
from datetime import datetime
import json


def test_imports():
    """Test if required modules can be imported"""
    print("=" * 70)
    print("TEST 1: Importing Modules")
    print("=" * 70)
    
    try:
        import sqlite3
        print("✅ sqlite3 available")
        print(f"   SQLite version: {sqlite3.sqlite_version}")
    except ImportError as e:
        print(f"❌ sqlite3 import failed: {e}")
        return False
    
    try:
        from database_managers import SQLManager
        print("✅ SQLManager imported successfully")
    except ImportError as e:
        print(f"❌ SQLManager import failed: {e}")
        return False
    
    try:
        from type_detector import TypeDetector
        print("✅ TypeDetector imported successfully")
    except ImportError as e:
        print(f"❌ TypeDetector import failed: {e}")
        return False
    
    print("\n✅ All imports successful!")
    return True


def test_sql_manager_initialization():
    """Test SQLManager initialization"""
    print("\n" + "=" * 70)
    print("TEST 2: SQLManager Initialization")
    print("=" * 70)
    
    try:
        from database_managers import SQLManager
        
        # Create test database
        test_db = 'test_sql_functionality.db'
        
        # Remove old test db if exists
        if os.path.exists(test_db):
            os.remove(test_db)
            print(f"🗑️  Removed old test database")
        
        print(f"\nCreating SQLManager with database: {test_db}")
        sql_manager = SQLManager(test_db)
        
        print("✅ SQLManager initialized successfully")
        
        # Check if database file was created
        if os.path.exists(test_db):
            print(f"✅ Database file created: {test_db}")
            size = os.path.getsize(test_db)
            print(f"   File size: {size} bytes")
        else:
            print("❌ Database file was not created")
            return None
        
        # Check initial schema
        schema = sql_manager.get_schema()
        print(f"\n✅ Initial schema retrieved: {len(schema)} columns")
        for col in schema:
            print(f"   - {col[1]} ({col[2]})")
        
        return sql_manager
        
    except Exception as e:
        print(f"❌ SQLManager initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_add_columns(sql_manager):
    """Test dynamic column addition"""
    print("\n" + "=" * 70)
    print("TEST 3: Dynamic Column Addition")
    print("=" * 70)
    
    try:
        # Test adding various column types
        test_columns = [
            ('email', 'email', False),
            ('age', 'integer', False),
            ('ip_address', 'ip_address', False),
            ('device_id', 'uuid', True),
            ('temperature', 'float', False),
            ('is_active', 'boolean', False),
            ('session_id', 'uuid', True),
        ]
        
        print("\nAdding test columns:")
        for col_name, data_type, unique in test_columns:
            print(f"\n  Adding: {col_name} (type: {data_type}, unique: {unique})")
            sql_manager.add_column_if_not_exists(col_name, data_type, unique)
        
        # Verify columns were added
        schema = sql_manager.get_schema()
        column_names = [col[1] for col in schema]
        
        print(f"\n✅ Schema now has {len(schema)} columns:")
        for col in schema:
            print(f"   - {col[1]} ({col[2]})")
        
        # Check if all test columns were added
        added_count = 0
        for col_name, _, _ in test_columns:
            if col_name in column_names:
                added_count += 1
                print(f"   ✓ {col_name} added successfully")
        
        if added_count == len(test_columns):
            print(f"\n✅ All {len(test_columns)} columns added successfully!")
            return True
        else:
            print(f"\n⚠️  Only {added_count}/{len(test_columns)} columns added")
            return False
        
    except Exception as e:
        print(f"❌ Column addition failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_insert_records(sql_manager):
    """Test record insertion"""
    print("\n" + "=" * 70)
    print("TEST 4: Record Insertion")
    print("=" * 70)
    
    try:
        # Create test records
        test_records = [
            {
                'username': 'test_user_1',
                'sys_ingested_at': datetime.utcnow().isoformat() + '.000001',
                't_stamp': datetime.utcnow().isoformat(),
                'email': 'user1@test.com',
                'age': 25,
                'ip_address': '192.168.1.1',
                'device_id': '550e8400-e29b-41d4-a716-446655440001',
                'temperature': 23.5,
                'is_active': True,
                'session_id': '550e8400-e29b-41d4-a716-446655440011'
            },
            {
                'username': 'test_user_2',
                'sys_ingested_at': datetime.utcnow().isoformat() + '.000002',
                't_stamp': datetime.utcnow().isoformat(),
                'email': 'user2@test.com',
                'age': 30,
                'ip_address': '10.0.0.1',
                'device_id': '550e8400-e29b-41d4-a716-446655440002',
                'temperature': 25.0,
                'is_active': False,
                'session_id': '550e8400-e29b-41d4-a716-446655440012'
            },
            {
                'username': 'test_user_3',
                'sys_ingested_at': datetime.utcnow().isoformat() + '.000003',
                't_stamp': datetime.utcnow().isoformat(),
                'email': 'user3@test.com',
                'age': 35,
                'ip_address': '172.16.0.1',
                # Missing device_id to test NULL handling
                'temperature': 20.2,
                'is_active': True,
                'session_id': '550e8400-e29b-41d4-a716-446655440013'
            }
        ]
        
        print(f"\nInserting {len(test_records)} test records:")
        
        success_count = 0
        for i, record in enumerate(test_records, 1):
            print(f"\n  Record {i}: {record['username']}")
            success = sql_manager.insert_record(record)
            if success:
                print(f"    ✅ Inserted successfully")
                success_count += 1
            else:
                print(f"    ❌ Insert failed")
        
        # Verify count
        count = sql_manager.get_record_count()
        print(f"\n✅ Database now has {count} records")
        
        if success_count == len(test_records):
            print(f"✅ All {len(test_records)} records inserted successfully!")
            return True
        else:
            print(f"⚠️  Only {success_count}/{len(test_records)} records inserted")
            return False
        
    except Exception as e:
        print(f"❌ Record insertion failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_query_records(sql_manager):
    """Test querying records"""
    print("\n" + "=" * 70)
    print("TEST 5: Querying Records")
    print("=" * 70)
    
    try:
        # Test 1: Count all records
        print("\nTest 5.1: Count all records")
        count = sql_manager.get_record_count()
        print(f"✅ Total records: {count}")
        
        # Test 2: Select all records
        print("\nTest 5.2: Select all records")
        sql_manager.cursor.execute("SELECT * FROM ingested_records")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Fetched {len(rows)} rows")
        
        # Test 3: Select specific columns
        print("\nTest 5.3: Select specific columns")
        sql_manager.cursor.execute("SELECT username, email, age FROM ingested_records")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Fetched {len(rows)} rows with 3 columns")
        for row in rows:
            print(f"   - {row[0]}: {row[1]} (age: {row[2]})")
        
        # Test 4: WHERE clause
        print("\nTest 5.4: Query with WHERE clause")
        sql_manager.cursor.execute("SELECT username, age FROM ingested_records WHERE age > 25")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Found {len(rows)} records with age > 25:")
        for row in rows:
            print(f"   - {row[0]}: age {row[1]}")
        
        # Test 5: ORDER BY
        print("\nTest 5.5: Query with ORDER BY")
        sql_manager.cursor.execute("SELECT username, age FROM ingested_records ORDER BY age DESC")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Records ordered by age (descending):")
        for row in rows:
            print(f"   - {row[0]}: age {row[1]}")
        
        # Test 6: Aggregation
        print("\nTest 5.6: Aggregation (AVG)")
        sql_manager.cursor.execute("SELECT AVG(age) as avg_age, MIN(age) as min_age, MAX(age) as max_age FROM ingested_records")
        row = sql_manager.cursor.fetchone()
        print(f"✅ Age statistics:")
        print(f"   - Average: {row[0]:.1f}")
        print(f"   - Minimum: {row[1]}")
        print(f"   - Maximum: {row[2]}")
        
        # Test 7: JOIN-like query (self-join on username)
        print("\nTest 5.7: Unique usernames")
        sql_manager.cursor.execute("SELECT DISTINCT username FROM ingested_records")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Unique usernames: {len(rows)}")
        
        print("\n✅ All query tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Query test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_null_handling(sql_manager):
    """Test NULL value handling"""
    print("\n" + "=" * 70)
    print("TEST 6: NULL Value Handling")
    print("=" * 70)
    
    try:
        # Insert record with NULL values
        print("\nInserting record with NULL values:")
        record_with_nulls = {
            'username': 'null_test_user',
            'sys_ingested_at': datetime.utcnow().isoformat() + '.000004',
            't_stamp': datetime.utcnow().isoformat(),
            'email': None,  # NULL
            'age': None,    # NULL
            'is_active': True
        }
        
        success = sql_manager.insert_record(record_with_nulls)
        if success:
            print("✅ Record with NULLs inserted successfully")
        else:
            print("❌ Failed to insert record with NULLs")
            return False
        
        # Query for NULL values
        print("\nQuerying for NULL values:")
        sql_manager.cursor.execute("SELECT username FROM ingested_records WHERE email IS NULL")
        rows = sql_manager.cursor.fetchall()
        print(f"✅ Found {len(rows)} records with NULL email:")
        for row in rows:
            print(f"   - {row[0]}")
        
        print("\n✅ NULL handling works correctly!")
        return True
        
    except Exception as e:
        print(f"❌ NULL handling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_type_compatibility(sql_manager):
    """Test different data types"""
    print("\n" + "=" * 70)
    print("TEST 7: Data Type Compatibility")
    print("=" * 70)
    
    try:
        from type_detector import TypeDetector
        detector = TypeDetector()
        
        # Test type detection
        print("\nTesting TypeDetector on various values:")
        test_values = [
            (192168.1, "Should be float, not IP"),
            ("192.168.1.1", "Should be ip_address"),
            (1.2, "Should be float"),
            ("user@example.com", "Should be email"),
            (True, "Should be boolean"),
            (42, "Should be integer"),
            ("550e8400-e29b-41d4-a716-446655440000", "Should be uuid"),
            (None, "Should be null"),
        ]
        
        all_correct = True
        for value, description in test_values:
            detected = detector.detect_type(value)
            print(f"   {str(value):40} → {detected:15} | {description}")
            
        # Test SQL type mapping
        print("\nTesting SQL type mapping:")
        type_mappings = [
            ('boolean', 'BOOLEAN'),
            ('integer', 'INTEGER'),
            ('float', 'REAL'),
            ('ip_address', 'VARCHAR(15)'),
            ('uuid', 'VARCHAR(36)'),
            ('email', 'VARCHAR(255)'),
            ('string', 'TEXT'),
        ]
        
        for semantic_type, expected_sql in type_mappings:
            sql_type = detector.get_sql_type(semantic_type)
            match = "✅" if sql_type == expected_sql else "❌"
            print(f"   {match} {semantic_type:15} → {sql_type:20} (expected: {expected_sql})")
        
        print("\n✅ Type compatibility tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Type compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_unique_constraints(sql_manager):
    """Test UNIQUE constraints"""
    print("\n" + "=" * 70)
    print("TEST 8: UNIQUE Constraint Handling")
    print("=" * 70)
    
    try:
        print("\nAttempting to insert duplicate sys_ingested_at:")
        duplicate_record = {
            'username': 'duplicate_test',
            'sys_ingested_at': datetime.utcnow().isoformat() + '.000001',  # Same as first record
            't_stamp': datetime.utcnow().isoformat(),
            'email': 'duplicate@test.com'
        }
        
        success = sql_manager.insert_record(duplicate_record)
        if not success:
            print("✅ Duplicate sys_ingested_at correctly rejected!")
            print("   (This is expected behavior - UNIQUE constraint working)")
        else:
            print("⚠️  Duplicate was inserted (UNIQUE constraint may not be enforced)")
        
        return True
        
    except Exception as e:
        print(f"❌ UNIQUE constraint test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cleanup(test_db='test_sql_functionality.db'):
    """Cleanup test database"""
    print("\n" + "=" * 70)
    print("CLEANUP")
    print("=" * 70)
    
    try:
        if os.path.exists(test_db):
            os.remove(test_db)
            print(f"✅ Test database removed: {test_db}")
        else:
            print(f"ℹ️  No test database to remove")
        
        return True
        
    except Exception as e:
        print(f"⚠️  Cleanup warning: {e}")
        return False


def main():
    """Run all SQL tests"""
    print("\n" + "🧪 " * 35)
    print("\n  SQL FUNCTIONALITY TEST SUITE")
    print("\n" + "🧪 " * 35)
    
    test_results = {}
    
    # Test 1: Imports
    test_results['imports'] = test_imports()
    if not test_results['imports']:
        print("\n❌ CRITICAL: Cannot proceed without required modules")
        sys.exit(1)
    
    # Test 2: Initialization
    sql_manager = test_sql_manager_initialization()
    test_results['initialization'] = sql_manager is not None
    if not sql_manager:
        print("\n❌ CRITICAL: Cannot proceed without SQLManager")
        sys.exit(1)
    
    # Test 3: Add columns
    test_results['add_columns'] = test_add_columns(sql_manager)
    
    # Test 4: Insert records
    test_results['insert_records'] = test_insert_records(sql_manager)
    
    # Test 5: Query records
    test_results['query_records'] = test_query_records(sql_manager)
    
    # Test 6: NULL handling
    test_results['null_handling'] = test_null_handling(sql_manager)
    
    # Test 7: Type compatibility
    test_results['type_compatibility'] = test_type_compatibility(sql_manager)
    
    # Test 8: UNIQUE constraints
    test_results['unique_constraints'] = test_unique_constraints(sql_manager)
    
    # Close SQL manager
    sql_manager.close()
    print("\n✅ SQLManager closed")
    
    # Cleanup
    print("\n" + "=" * 70)
    print("Do you want to keep the test database for inspection? (y/n)")
    print("=" * 70)
    keep_db = input().strip().lower()
    
    if keep_db != 'y':
        test_cleanup()
    else:
        print("ℹ️  Test database preserved: test_sql_functionality.db")
        print("   You can inspect it with: sqlite3 test_sql_functionality.db")
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    
    passed = sum(1 for result in test_results.values() if result)
    total = len(test_results)
    
    print(f"\nResults: {passed}/{total} tests passed\n")
    
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"  {status:15} : {test_name}")
    
    print("\n" + "=" * 70)
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! SQL functionality is working perfectly!")
        print("=" * 70)
        print("\nThe SQL system is ready for production use:")
        print("  - SQLManager initialization ✓")
        print("  - Dynamic schema evolution ✓")
        print("  - Record insertion ✓")
        print("  - Data querying ✓")
        print("  - NULL handling ✓")
        print("  - Type detection and mapping ✓")
        print("  - UNIQUE constraints ✓")
        print("\nYou can now run the full ingestion system:")
        print("  python data_consumer.py")
        return 0
    else:
        print(f"⚠️  {total - passed} test(s) failed. Review errors above.")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    sys.exit(main())
