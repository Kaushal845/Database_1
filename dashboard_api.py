"""
Dashboard API - Enhanced version with better error handling and startup validation
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import json
import time
import traceback
from pathlib import Path

from ingestion_pipeline import IngestionPipeline
from metadata_store import MetadataStore
from database_managers import SQLManager, MongoDBManager
from transaction_coordinator import TransactionCoordinator
from logging_utils import get_logger


logger = get_logger("dashboard_api")
app = FastAPI(title="Hybrid DB Dashboard API", version="1.0.0")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables for initialized components
pipeline = None
transaction_coordinator = None
initialization_error = None

# Query execution history (in-memory, capped at 500 entries)
query_history: List[Dict[str, Any]] = []
QUERY_HISTORY_MAX = 500


def initialize_system():
    """Initialize pipeline with transaction coordinator"""
    global pipeline, transaction_coordinator, initialization_error

    try:
        logger.info("Initializing dashboard system...")

        # Initialize pipeline with transaction support enabled
        pipeline = IngestionPipeline(use_transactions=True)
        logger.info(f"Pipeline initialized with ACID transaction support")
        logger.info(f"Current records: {pipeline.stats['total_processed']} processed")

        # Use the pipeline's transaction coordinator
        transaction_coordinator = pipeline.transaction_coordinator
        if transaction_coordinator:
            logger.info("Transaction coordinator ready - All CRUD operations are atomic")
        else:
            logger.warning("Transaction coordinator not available - CRUD operations lack ACID guarantees")

        logger.info("System initialization complete ✓")
        return True

    except Exception as e:
        initialization_error = str(e)
        logger.error(f"System initialization failed: {e}", exc_info=True)
        return False


# Initialize on startup
@app.on_event("startup")
async def startup_event():
    """Run initialization when API starts"""
    success = initialize_system()
    if not success:
        logger.warning(f"System started with initialization errors: {initialization_error}")


@app.get("/")
async def root():
    """Root endpoint with system status"""
    return {
        "service": "Hybrid Database Dashboard API",
        "version": "1.0.0",
        "status": "ready" if pipeline else "initializing",
        "initialization_error": initialization_error,
        "endpoints": [
            "/api/dashboard/summary",
            "/api/dashboard/records",
            "/api/dashboard/session",
            "/api/dashboard/entities",
            "/api/dashboard/field-placements",
            "/api/query",
            "/api/transaction/*"
        ]
    }


@app.get("/health")
async def health_check():
    """Detailed health check endpoint"""
    if not pipeline:
        return {
            "status": "error",
            "error": initialization_error or "Pipeline not initialized",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    try:
        sql_count = pipeline.sql_manager.get_record_count()
        mongo_count = pipeline.mongo_manager.get_record_count('ingested_records')

        return {
            "status": "healthy",
            "service": "dashboard_api",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database_status": {
                "sql_records": sql_count,
                "mongo_records": mongo_count,
                "sql_connection": "ok",
                "mongo_connection": "ok" if not pipeline.mongo_manager.using_memory_fallback else "fallback"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "degraded",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


@app.get("/api/dashboard/summary")
async def get_dashboard_summary() -> Dict[str, Any]:
    """GET /api/dashboard/summary - Return overall statistics"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        stats = pipeline.get_statistics()
        metadata = pipeline.metadata_store.metadata

        # Count records by backend
        sql_count = pipeline.sql_manager.get_record_count()
        mongo_count = pipeline.mongo_manager.get_record_count('ingested_records')
        buffer_count = pipeline.mongo_manager.get_record_count('buffer_records')

        # Count fields by placement
        field_mappings = metadata.get('field_mappings', {})
        placement_counts = {
            'SQL': 0,
            'MongoDB': 0,
            'Both': 0,
            'Buffer': 0
        }

        for field_name, mapping in field_mappings.items():
            backend = mapping.get('backend', 'Unknown')
            if backend in placement_counts:
                placement_counts[backend] += 1

        # Get child tables and collections
        sql_child_tables = pipeline.sql_manager.list_child_tables()
        mongo_collections = pipeline.mongo_manager.list_collections()

        # Calculate actual pipeline statistics from database state
        # Instead of using session-based stats, derive from actual counts
        actual_pipeline_stats = {
            "total_processed": sql_count,  # SQL always has all core records
            "total_inserts": sql_count,
            "buffer_inserts": buffer_count,
            "errors": 0  # Could be tracked separately if needed
        }

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_records": {
                    "total": sql_count,
                    "buffered": buffer_count,
                    "total_processed": sql_count
                },
                "field_count": {
                    "total_fields": len(metadata.get('fields', {})),
                    "mapped_fields": len(field_mappings)
                },
                "database_objects": {
                    "total_tables": len(sql_child_tables) + len(mongo_collections)
                },
                "pipeline_stats": actual_pipeline_stats
            }
        }
    except Exception as e:
        logger.error(f"Error getting dashboard summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/api/dashboard/records")
async def get_dashboard_records(
    limit: int = 50,
    offset: int = 0,
    username: Optional[str] = None
) -> Dict[str, Any]:
    """GET /api/dashboard/records - Paginated view of ingested records"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        limit = max(1, min(limit, 1000))
        offset = max(0, offset)

        filters = {}
        if username:
            filters['username'] = {'$starts_with': username.strip()}

        # Get all fields for maximum visibility
        all_fields = list(pipeline.metadata_store.metadata.get('field_mappings', {}).keys())

        # If no fields are mapped yet, use default core fields
        if not all_fields:
            all_fields = ['username', 'sys_ingested_at', 't_stamp']
            logger.info("No field mappings found, using default fields")

        # Filter to only include fields that have valid mappings and exist
        # This prevents querying fields from SQL that don't exist as columns
        sql_columns = set(pipeline.sql_manager.get_existing_columns("ingested_records"))
        valid_fields = []

        for field in all_fields:
            mapping = pipeline.metadata_store.get_field_mapping(field)
            if mapping:
                sql_table = mapping.get("sql_table")
                mongo_collection = mapping.get("mongo_collection")

                # Include field if it's in MongoDB, or if it's in SQL and the column exists
                if mongo_collection:
                    valid_fields.append(field)
                elif sql_table == "ingested_records" and field in sql_columns:
                    valid_fields.append(field)

        # Ensure core fields are included
        for core_field in ['username', 'sys_ingested_at', 't_stamp']:
            if core_field not in valid_fields and core_field in sql_columns:
                valid_fields.append(core_field)

        logger.debug(f"Querying with {len(valid_fields)} valid fields from {len(all_fields)} total")

        # Fetch one extra record to determine whether another page exists.
        fetch_limit = limit + offset + 1

        # Execute read query through query engine
        read_result = pipeline.query_engine.execute({
            'operation': 'read',
            'fields': valid_fields,
            'filters': filters,
            'limit': fetch_limit
        })

        if not read_result.get('success'):
            error_msg = read_result.get('error', 'Read failed')
            logger.error(f"Query execution failed: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)

        all_records = read_result.get('records', [])

        # Apply offset and limit for current page.
        records = all_records[offset:offset + limit]
        has_more = len(all_records) > (offset + limit)

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total_returned": len(records),
                "has_more": has_more
            },
            "records": records
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dashboard records: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.get("/api/dashboard/session")
async def get_session_info() -> Dict[str, Any]:
    """GET /api/dashboard/session - Get active session information"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        from datetime import datetime, timezone

        metadata = pipeline.metadata_store.metadata
        session_start = metadata.get('session_start')
        last_updated = metadata.get('last_updated')

        # Calculate session duration
        session_duration = None
        if session_start:
            try:
                # Parse ISO format timestamp
                if isinstance(session_start, str):
                    # Handle both with and without timezone
                    if '+' in session_start or session_start.endswith('Z'):
                        start_time = datetime.fromisoformat(session_start.replace('Z', '+00:00'))
                    else:
                        start_time = datetime.fromisoformat(session_start).replace(tzinfo=timezone.utc)
                else:
                    start_time = session_start

                current_time = datetime.now(timezone.utc)
                duration_seconds = (current_time - start_time).total_seconds()

                # Format duration as human-readable
                days = int(duration_seconds // 86400)
                hours = int((duration_seconds % 86400) // 3600)
                minutes = int((duration_seconds % 3600) // 60)

                if days > 0:
                    session_duration = f"{days}d {hours}h {minutes}m"
                elif hours > 0:
                    session_duration = f"{hours}h {minutes}m"
                else:
                    session_duration = f"{minutes}m"
            except Exception as e:
                logger.warning(f"Failed to calculate session duration: {e}")
                session_duration = "Unknown"

        # Get activity statistics
        total_records = metadata.get('total_records', 0)
        total_fields = len(metadata.get('fields', {}))

        # Count entities
        sql_entities = len(metadata.get('normalization', {}).get('child_tables', {}))
        mongo_entities = len(metadata.get('mongo_strategy', {}).get('entities', {}))

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session": {
                "session_start": session_start,
                "last_updated": last_updated,
                "duration": session_duration,
                "activity": {
                    "total_records": total_records,
                    "total_fields_discovered": total_fields,
                    "total_entities_discovered": sql_entities + mongo_entities
                }
            }
        }
    except Exception as e:
        logger.error(f"Error getting session info: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/transaction-status")
async def get_transaction_status() -> Dict[str, Any]:
    """GET /api/dashboard/transaction-status - Get transaction system status"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        transaction_enabled = pipeline.use_transactions and transaction_coordinator is not None

        if not transaction_enabled:
            return {
                "success": True,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "transaction_system": {
                    "enabled": False,
                    "message": "Transaction system is disabled - CRUD operations lack ACID guarantees"
                }
            }

        # Get active transactions
        active_transactions = []
        if transaction_coordinator:
            active_transactions = transaction_coordinator.list_active_transactions()

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_system": {
                "enabled": True,
                "message": "All CRUD operations are automatically wrapped in ACID transactions",
                "active_transactions": len(active_transactions),
                "transactions": active_transactions
            }
        }
    except Exception as e:
        logger.error(f"Error getting transaction status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def _enrich_field_metadata(field_name: str, field_meta: Dict[str, Any], total_instances: int) -> Dict[str, Any]:
    """Enrich field metadata with type inference and coverage calculations."""
    
    # Get field statistics
    appearances = field_meta.get('appearances', 0)
    coverage = (appearances / total_instances * 100) if total_instances > 0 else 0
    
    # Infer field type from type_counts
    type_counts = field_meta.get('type_counts', {})
    inferred_type = 'unknown'
    
    if type_counts:
        # Get the most common type
        most_common_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else 'unknown'
        inferred_type = most_common_type
    
    # Get sample values
    sample_value = field_meta.get('sample_value', None)
    first_seen = field_meta.get('first_seen')
    last_seen = field_meta.get('last_seen')
    
    # Determine cardinality
    cardinality = 'optional' if coverage < 100 else 'required'
    
    return {
        "name": field_name,
        "type": inferred_type,
        "cardinality": cardinality,
        "coverage": round(coverage, 1),
        "instances": appearances,
        "sample_value": sample_value,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "type_distribution": type_counts
    }


@app.get("/api/dashboard/entities")
async def get_entity_catalog() -> Dict[str, Any]:
    """GET /api/dashboard/entities - Get catalog of logical entities with detailed field information"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        metadata = pipeline.metadata_store.metadata
        fields_metadata = metadata.get('fields', {})
        total_records = metadata.get('total_records', 0)

        entities = []

        # Get normalized SQL entities (repeating groups)
        sql_child_tables = metadata.get('normalization', {}).get('child_tables', {})
        for table_name, table_info in sql_child_tables.items():
            entity_path = table_info.get('entity_path', table_name)

            # Get instance count from SQL
            try:
                cursor = pipeline.sql_manager.cursor
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                instance_count = cursor.fetchone()[0]
            except:
                instance_count = 0

            # Enrich fields with metadata
            column_names = table_info.get('columns', [])
            enriched_fields = []
            
            for col in column_names:
                # Try to find field metadata with various prefixes
                field_meta = fields_metadata.get(col, {})
                if not field_meta:
                    # Try with entity prefix
                    field_meta = fields_metadata.get(f"{entity_path}.{col}", {})
                
                enriched_field = _enrich_field_metadata(col, field_meta, instance_count)
                enriched_fields.append(enriched_field)

            entities.append({
                "name": entity_path,
                "type": "repeating",
                "fields": enriched_fields,
                "field_count": len(enriched_fields),
                "instance_count": instance_count,
                "registered_at": table_info.get('registered_at'),
                "relationships": {
                    "parent": None,
                    "children": []
                }
            })

        # Get MongoDB entities (nested/embedded objects)
        mongo_entities = metadata.get('mongo_strategy', {}).get('entities', {})
        for entity_name, entity_info in mongo_entities.items():
            # Skip if already added as SQL entity
            if any(e['name'] == entity_name for e in entities):
                continue

            # For embedded entities, get field list from metadata
            enriched_fields = []
            entity_field_names = []
            
            for field_name, field_meta in fields_metadata.items():
                if field_name.startswith(f"{entity_name}."):
                    clean_field = field_name.replace(f"{entity_name}.", "")
                    entity_field_names.append(clean_field)
                    
                    enriched_field = _enrich_field_metadata(clean_field, field_meta, total_records)
                    enriched_fields.append(enriched_field)

            # Get instance count
            mode = entity_info.get('mode', 'embed')
            instance_count = 0

            if mode == 'embed':
                try:
                    cursor = pipeline.sql_manager.cursor
                    cursor.execute("SELECT COUNT(*) FROM ingested_records")
                    instance_count = cursor.fetchone()[0]
                except:
                    instance_count = 0
            elif mode == 'reference':
                collection = entity_info.get('collection')
                if collection:
                    try:
                        instance_count = pipeline.mongo_manager.get_record_count(collection)
                    except:
                        instance_count = 0

            # Detect parent entity (if entity name contains a dot, it's a child)
            parent_entity = None
            if '.' in entity_name:
                parent_entity = entity_name.split('.')[0]

            entities.append({
                "name": entity_name,
                "type": "nested",
                "fields": enriched_fields,
                "field_count": len(enriched_fields),
                "instance_count": instance_count,
                "registered_at": entity_info.get('registered_at'),
                "relationships": {
                    "parent": parent_entity,
                    "children": []
                }
            })

        # Build parent-child relationships
        for entity in entities:
            if entity["relationships"]["parent"]:
                parent_name = entity["relationships"]["parent"]
                parent = next((e for e in entities if e["name"] == parent_name), None)
                if parent:
                    parent["relationships"]["children"].append(entity["name"])

        # Sort by name
        entities.sort(key=lambda x: x['name'])

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_entities": len(entities),
            "entities": entities
        }
    except Exception as e:
        logger.error(f"Error getting entity catalog: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/field-placements")
async def get_field_placements() -> Dict[str, Any]:
    """GET /api/dashboard/field-placements - Show field statistics without exposing backend details"""
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        metadata = pipeline.metadata_store.metadata
        field_mappings = metadata.get('field_mappings', {})
        fields_metadata = metadata.get('fields', {})

        placements = []
        for field_name, mapping in field_mappings.items():
            field_meta = fields_metadata.get(field_name, {})

            # Only expose user-facing information, no backend details
            placement_info = {
                "field_name": field_name,
                "status": mapping.get('status', 'unknown'),
                "statistics": {
                    "count": field_meta.get('appearances', 0),  # Field count stored as 'appearances'
                    "first_seen": field_meta.get('first_seen'),
                    "last_seen": field_meta.get('last_seen'),
                    "semantic_types": field_meta.get('type_counts', {})  # Type counts, not semantic_types
                }
            }

            placements.append(placement_info)

        # Sort by field name
        placements.sort(key=lambda x: x['field_name'])

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_fields": len(placements),
            "placements": placements
        }
    except Exception as e:
        logger.error(f"Error getting field placements: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query")
async def execute_query(request: Dict[str, Any]) -> Dict[str, Any]:
    """POST /api/query - Execute CRUD operations with query history tracking"""
    global query_history
    if not pipeline:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    start_time = time.perf_counter()
    try:
        logger.info(f"Executing query: operation={request.get('operation')}")
        result = pipeline.execute_crud(request)
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        # Remove backend-specific details from response
        sanitized_result = {
            "success": result.get('success', False),
            "error": result.get('error')
        }

        # Include user-facing data only
        if 'records' in result:
            sanitized_result['records'] = result['records']
        if 'count' in result:
            sanitized_result['count'] = result['count']
        if 'inserted' in result:
            sanitized_result['inserted'] = result['inserted']

        # Combine SQL and MongoDB deletion counts into one
        if 'sql_deleted' in result or 'mongo_deleted' in result:
            sanitized_result['deleted'] = result.get('sql_deleted', 0) + result.get('mongo_deleted', 0)

        # Record query in history
        history_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": request.get('operation', 'unknown'),
            "filters": request.get('filters', {}),
            "fields": request.get('fields'),
            "latency_ms": elapsed_ms,
            "success": result.get('success', False),
            "record_count": result.get('count', result.get('updated', result.get('inserted', 0))),
            "error": result.get('error'),
        }
        query_history.append(history_entry)
        if len(query_history) > QUERY_HISTORY_MAX:
            query_history = query_history[-QUERY_HISTORY_MAX:]

        return {
            "success": result.get('success', False),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "result": sanitized_result
        }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        # Record failed query in history too
        query_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "operation": request.get('operation', 'unknown'),
            "filters": request.get('filters', {}),
            "latency_ms": elapsed_ms,
            "success": False,
            "error": str(e),
        })
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/query-history")
async def get_query_history(limit: int = 100) -> Dict[str, Any]:
    """GET /api/dashboard/query-history - View query execution history"""
    limit = max(1, min(limit, 500))
    recent = list(reversed(query_history[-limit:]))  # Most recent first
    return {
        "success": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total": len(query_history),
        "showing": len(recent),
        "history": recent
    }


@app.delete("/api/dashboard/query-history")
async def clear_query_history() -> Dict[str, Any]:
    """DELETE /api/dashboard/query-history - Clear query execution history"""
    global query_history
    count = len(query_history)
    query_history = []
    return {
        "success": True,
        "cleared": count,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/api/dashboard/benchmark-results")
async def get_benchmark_results() -> Dict[str, Any]:
    """GET /api/dashboard/benchmark-results - Get performance & comparative benchmark results"""
    docs_dir = Path("docs")
    results = {}

    # Load performance benchmark results
    perf_path = docs_dir / "performance_benchmark_results.json"
    if perf_path.exists():
        try:
            results["performance"] = json.loads(perf_path.read_text(encoding="utf-8"))
        except Exception as e:
            results["performance"] = {"error": str(e)}

    # Load comparative benchmark results
    comp_path = docs_dir / "comparative_benchmark_results.json"
    if comp_path.exists():
        try:
            results["comparative"] = json.loads(comp_path.read_text(encoding="utf-8"))
        except Exception as e:
            results["comparative"] = {"error": str(e)}

    # Load legacy ingestion benchmark results
    ingest_path = docs_dir / "ingestion_benchmark_results.json"
    if ingest_path.exists():
        try:
            results["ingestion"] = json.loads(ingest_path.read_text(encoding="utf-8"))
        except Exception as e:
            results["ingestion"] = {"error": str(e)}

    return {
        "success": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "has_data": len(results) > 0,
        "benchmarks": results
    }

# Transaction API endpoints...
@app.post("/api/transaction/begin")
async def begin_transaction() -> Dict[str, Any]:
    """POST /api/transaction/begin - Start a new transaction"""
    if not transaction_coordinator:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        tx_id = transaction_coordinator.begin_transaction()
        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tx_id": tx_id,
            "message": "Transaction started successfully"
        }
    except Exception as e:
        logger.error(f"Error beginning transaction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/transaction/{tx_id}/commit")
async def commit_transaction(tx_id: str) -> Dict[str, Any]:
    """POST /api/transaction/{tx_id}/commit - Commit transaction"""
    if not transaction_coordinator:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        success, error = transaction_coordinator.commit(tx_id)

        if not success:
            return {
                "success": False,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tx_id": tx_id,
                "error": error
            }

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tx_id": tx_id,
            "message": "Transaction committed successfully"
        }
    except Exception as e:
        logger.error(f"Error committing transaction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/transaction/list")
async def list_transactions() -> Dict[str, Any]:
    """GET /api/transaction/list - List all active transactions"""
    if not transaction_coordinator:
        raise HTTPException(status_code=503, detail=f"System not initialized: {initialization_error}")

    try:
        transactions = transaction_coordinator.list_active_transactions()

        return {
            "success": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "count": len(transactions),
            "transactions": transactions
        }
    except Exception as e:
        logger.error(f"Error listing transactions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    print("=" * 70)
    print("Starting Hybrid Database Dashboard API")
    print("=" * 70)
    print("\nInitializing system...")

    if initialize_system():
        print("✓ System initialized successfully")
        print(f"✓ SQL records: {pipeline.sql_manager.get_record_count()}")
        print(f"✓ MongoDB records: {pipeline.mongo_manager.get_record_count('ingested_records')}")
        print(f"✓ Total processed: {pipeline.stats['total_processed']}")
    else:
        print(f"⚠ System initialized with errors: {initialization_error}")
        print("  API will start but may have limited functionality")

    print("\n" + "=" * 70)
    print("API starting on http://localhost:8000")
    print("Documentation: http://localhost:8000/docs")
    print("Health check: http://localhost:8000/health")
    print("=" * 70)
    print()

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
