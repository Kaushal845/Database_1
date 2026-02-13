"""
Quick test run of the ingestion system without prompts
"""
from data_consumer import DataConsumer

print("=" * 70)
print("QUICK TEST - AUTONOMOUS DATA INGESTION SYSTEM")
print("=" * 70)

# Configuration
API_URL = 'http://127.0.0.1:8000'
BATCH_SIZE = 50
TOTAL_BATCHES = 25

# Test API connection
print(f"\nTesting connection to {API_URL}...")
try:
    import requests
    response = requests.get(API_URL, timeout=5)
    if response.status_code == 200:
        print("✓ API server is reachable")
    else:
        print(f"✗ API returned status {response.status_code}")
        exit(1)
except Exception as e:
    print(f"✗ Cannot connect to API: {e}")
    exit(1)

# Create consumer and start ingestion
print(f"\nConfiguration:")
print(f"  - Batch size: {BATCH_SIZE} records")
print(f"  - Total batches: {TOTAL_BATCHES}")
print(f"  - Total records: {BATCH_SIZE * TOTAL_BATCHES}")
print(f"  - API URL: {API_URL}")
print("\nStarting ingestion...\n")

consumer = DataConsumer(api_url=API_URL)
consumer.consume_continuous(
    batch_size=BATCH_SIZE,
    total_batches=TOTAL_BATCHES,
    delay=0.5
)

print("\n" + "=" * 70)
print("Test complete!")
print("=" * 70)
