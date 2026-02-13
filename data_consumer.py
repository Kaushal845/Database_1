"""
Data Consumer - Fetches records from FastAPI stream and ingests them
"""
import requests
import json
import time
from typing import Optional
from ingestion_pipeline import IngestionPipeline


class DataConsumer:
    """
    Consumes data from the FastAPI streaming endpoint and feeds it to the ingestion pipeline.
    """
    
    def __init__(self, 
                 api_url='http://127.0.0.1:8000',
                 pipeline: Optional[IngestionPipeline] = None):
        self.api_url = api_url
        self.pipeline = pipeline or IngestionPipeline()
        self.is_running = False
    
    def fetch_single_record(self) -> Optional[dict]:
        """
        Fetch a single record from the API.
        """
        try:
            response = requests.get(f"{self.api_url}/", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"[Consumer] Error: HTTP {response.status_code}")
                return None
        except Exception as e:
            print(f"[Consumer] Error fetching record: {e}")
            return None
    
    def fetch_batch(self, count: int):
        """
        Fetch multiple records from the streaming endpoint.
        Uses Server-Sent Events (SSE) format.
        """
        try:
            url = f"{self.api_url}/record/{count}"
            print(f"[Consumer] Fetching {count} records from {url}")
            
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code != 200:
                print(f"[Consumer] Error: HTTP {response.status_code}")
                return
            
            # Parse SSE stream
            records_processed = 0
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    
                    # SSE format: "data: {json}"
                    if line_str.startswith('data: '):
                        json_str = line_str[6:]  # Remove "data: " prefix
                        try:
                            record = json.loads(json_str)
                            self.pipeline.ingest_record(record)
                            records_processed += 1
                            
                            # Progress update every 100 records
                            if records_processed % 100 == 0:
                                print(f"[Consumer] Fetched and ingested {records_processed}/{count} records")
                        
                        except json.JSONDecodeError as e:
                            print(f"[Consumer] JSON decode error: {e}")
            
            print(f"[Consumer] Batch complete: {records_processed} records ingested")
        
        except Exception as e:
            print(f"[Consumer] Error fetching batch: {e}")
    
    def consume_continuous(self, batch_size: int = 100, total_batches: int = 10, delay: float = 1.0):
        """
        Continuously fetch and ingest data in batches.
        
        Args:
            batch_size: Number of records per batch
            total_batches: Total number of batches to fetch
            delay: Delay between batches (seconds)
        """
        self.is_running = True
        print(f"[Consumer] Starting continuous consumption: {total_batches} batches of {batch_size} records")
        
        try:
            for batch_num in range(1, total_batches + 1):
                if not self.is_running:
                    break
                
                print(f"\n[Consumer] === Batch {batch_num}/{total_batches} ===")
                self.fetch_batch(batch_size)
                
                if batch_num < total_batches:
                    print(f"[Consumer] Waiting {delay}s before next batch...")
                    time.sleep(delay)
            
            print("\n[Consumer] Consumption complete!")
            
            # Print final statistics
            stats = self.pipeline.get_statistics()
            print("\n=== Final Statistics ===")
            print(f"Total records processed: {stats['pipeline']['total_processed']}")
            print(f"SQL inserts: {stats['pipeline']['sql_inserts']}")
            print(f"MongoDB inserts: {stats['pipeline']['mongodb_inserts']}")
            print(f"Errors: {stats['pipeline']['errors']}")
            print(f"Unique fields discovered: {stats['metadata']['unique_fields']}")
            print(f"Normalization rules: {stats['metadata']['normalization_rules']}")
            print(f"Placement decisions made: {stats['metadata']['placement_decisions']}")
        
        except KeyboardInterrupt:
            print("\n[Consumer] Interrupted by user")
            self.is_running = False
        
        finally:
            self.pipeline.close()
    
    def stop(self):
        """Stop continuous consumption"""
        self.is_running = False


# Main execution
if __name__ == "__main__":
    import sys
    
    print("=" * 70)
    print("AUTONOMOUS DATA INGESTION SYSTEM")
    print("=" * 70)
    print("\nThis system will:")
    print("1. Fetch JSON records from the FastAPI server")
    print("2. Normalize field names dynamically")
    print("3. Track field frequency and type stability")
    print("4. Decide SQL vs MongoDB placement automatically")
    print("5. Store data with bi-temporal timestamps")
    print("\nPress Ctrl+C to stop at any time")
    print("=" * 70)
    
    # Configuration
    API_URL = 'http://127.0.0.1:8000'
    
    # Check if custom parameters provided
    if len(sys.argv) > 1:
        batch_size = int(sys.argv[1])
    else:
        batch_size = 100
    
    if len(sys.argv) > 2:
        total_batches = int(sys.argv[2])
    else:
        total_batches = 10
    
    # Test API connection
    print(f"\n[Consumer] Testing connection to {API_URL}...")
    try:
        response = requests.get(API_URL, timeout=5)
        if response.status_code == 200:
            print("[Consumer] ✓ API server is reachable")
        else:
            print(f"[Consumer] ✗ API returned status {response.status_code}")
            print("[Consumer] Make sure the FastAPI server is running:")
            print("[Consumer]   uvicorn main:app --reload --port 8000")
            sys.exit(1)
    except Exception as e:
        print(f"[Consumer] ✗ Cannot connect to API: {e}")
        print("[Consumer] Make sure the FastAPI server is running:")
        print("[Consumer]   uvicorn main:app --reload --port 8000")
        sys.exit(1)
    
    # Create consumer and start ingestion
    print(f"\n[Consumer] Configuration:")
    print(f"  - Batch size: {batch_size} records")
    print(f"  - Total batches: {total_batches}")
    print(f"  - Total records: {batch_size * total_batches}")
    print(f"  - API URL: {API_URL}")
    print()
    
    input("Press Enter to start ingestion...")
    print()
    
    consumer = DataConsumer(api_url=API_URL)
    consumer.consume_continuous(
        batch_size=batch_size,
        total_batches=total_batches,
        delay=0.5
    )
