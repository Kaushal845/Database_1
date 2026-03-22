"""
Data Consumer - Fetches records from FastAPI stream and ingests them
"""
import requests
import json
import time
from typing import Optional, Dict, Any
from ingestion_pipeline import IngestionPipeline
from logging_utils import get_logger


logger = get_logger("consumer")


class DataConsumer:
    """
    Consumes data from the FastAPI streaming endpoint and feeds it to the ingestion pipeline.
    """
    
    def __init__(self, 
                 api_url='http://127.0.0.1:8000',
                 pipeline: Optional[IngestionPipeline] = None,
                 schema: Optional[Dict[str, Any]] = None):
        self.api_url = api_url
        self.pipeline = pipeline or IngestionPipeline()
        self.is_running = False
        self._closed = False

        if schema:
            schema_info = self.pipeline.register_schema(schema)
            logger.info("Registered schema version %s", schema_info['schema_version'])
    
    def fetch_single_record(self) -> Optional[dict]:
        """
        Fetch a single record from the API.
        """
        try:
            response = requests.get(f"{self.api_url}/", timeout=5)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error("Single record fetch failed with HTTP %s", response.status_code)
                return None
        except Exception as e:
            logger.error("Single record fetch exception: %s", e)
            return None
    
    def fetch_batch(self, count: int):
        """
        Fetch multiple records from the streaming endpoint.
        Uses Server-Sent Events (SSE) format.
        """
        try:
            url = f"{self.api_url}/record/{count}"
            logger.info("Fetching batch of %s records from %s", count, url)
            
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code != 200:
                logger.error("Batch fetch failed with HTTP %s", response.status_code)
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
                                logger.info("Fetched and ingested %s/%s records", records_processed, count)
                        
                        except json.JSONDecodeError as e:
                            logger.warning("Skipping malformed SSE payload: %s", e)
            
            logger.info("Batch complete: %s records ingested", records_processed)
        
        except Exception as e:
            logger.error("Batch fetch exception: %s", e)
    
    def consume_continuous(
        self,
        batch_size: int = 100,
        total_batches: int = 10,
        delay: float = 1.0,
        close_on_finish: bool = True,
    ):
        """
        Continuously fetch and ingest data in batches.
        
        Args:
            batch_size: Number of records per batch
            total_batches: Total number of batches to fetch
            delay: Delay between batches (seconds)
            close_on_finish: Close the pipeline after consumption completes
        """
        self.is_running = True
        logger.info(
            "Starting continuous consumption: batches=%s batch_size=%s delay=%ss",
            total_batches,
            batch_size,
            delay,
        )
        
        try:
            for batch_num in range(1, total_batches + 1):
                if not self.is_running:
                    break
                
                logger.info("Starting batch %s/%s", batch_num, total_batches)
                self.fetch_batch(batch_size)
                
                if batch_num < total_batches:
                    logger.debug("Sleeping for %ss before next batch", delay)
                    time.sleep(delay)
            
            logger.info("Consumption complete")
            
            # Print final statistics
            stats = self.pipeline.get_statistics()
            logger.info(
                "Final stats: processed=%s sql_inserts=%s mongo_inserts=%s buffer_inserts=%s errors=%s",
                stats['pipeline']['total_processed'],
                stats['pipeline']['sql_inserts'],
                stats['pipeline']['mongodb_inserts'],
                stats['pipeline'].get('buffer_inserts', 0),
                stats['pipeline']['errors'],
            )
            logger.info(
                "Metadata summary: unique_fields=%s placement_decisions=%s",
                stats['metadata']['unique_fields'],
                stats['metadata']['placement_decisions'],
            )
        
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
            self.is_running = False
        
        finally:
            if close_on_finish:
                self.close()
    
    def stop(self):
        """Stop continuous consumption"""
        self.is_running = False

    def close(self):
        """Close underlying resources once."""
        if self._closed:
            return
        self.pipeline.close()
        self._closed = True

    def execute_query(self, query_request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute Assignment-2 metadata-driven CRUD query."""
        return self.pipeline.execute_crud(query_request)


# Main execution
if __name__ == "__main__":
    import sys
    
    print("=" * 70)
    print("AUTONOMOUS DATA INGESTION SYSTEM")
    print("=" * 70)
    print("\nThis system will:")
    print("1. Fetch JSON records from the FastAPI server")
    print("2. Track field frequency and type stability")
    print("3. Decide SQL vs MongoDB placement automatically")
    print("4. Store data with bi-temporal timestamps")
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
        total_batches = 5
    
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
    
    schema = None
    if len(sys.argv) > 3:
        schema_path = sys.argv[3]
        try:
            with open(schema_path, 'r', encoding='utf-8') as schema_file:
                schema = json.load(schema_file)
            print(f"[Consumer] Loaded schema from: {schema_path}")
        except Exception as error:
            print(f"[Consumer] Warning: failed to load schema file {schema_path}: {error}")

    # Create consumer and start ingestion
    print(f"\n[Consumer] Configuration:")
    print(f"  - Batch size: {batch_size} records")
    print(f"  - Total batches: {total_batches}")
    print(f"  - Total records: {batch_size * total_batches}")
    print(f"  - API URL: {API_URL}")
    if schema:
        print("  - Schema registration: Enabled")
    print()
    
    input("Press Enter to start ingestion...")
    print()
    
    consumer = DataConsumer(api_url=API_URL, schema=schema)
    consumer.consume_continuous(
        batch_size=batch_size,
        total_batches=total_batches,
        delay=0.5
    )
