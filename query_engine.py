"""
Metadata-driven query engine for Assignment-2 CRUD operations.
"""
from typing import Any, Callable, Dict, List, Optional
import re

from database_managers import SQLManager, MongoDBManager
from metadata_store import MetadataStore
from logging_utils import get_logger


logger = get_logger("query_engine")


class MetadataDrivenQueryEngine:
    """Generate and execute CRUD operations based on metadata mappings."""

    def __init__(
        self,
        metadata_store: MetadataStore,
        sql_manager: SQLManager,
        mongo_manager: MongoDBManager,
        ingest_callback: Callable[[Dict[str, Any]], bool],
        transaction_coordinator=None,
    ):
        self.metadata_store = metadata_store
        self.sql_manager = sql_manager
        self.mongo_manager = mongo_manager
        self.ingest_callback = ingest_callback
        self.transaction_coordinator = transaction_coordinator
        self.use_transactions = transaction_coordinator is not None

    def execute(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute CRUD operation with automatic transaction protection"""
        operation = request.get("operation", "").lower().strip()
        logger.info("Executing CRUD operation: %s (transactional=%s)", operation, self.use_transactions)

        # If transactions are enabled, wrap the operation in a transaction
        if self.use_transactions:
            return self._execute_transactional(request, operation)

        # Otherwise, execute without transaction (legacy mode)
        return self._execute_non_transactional(request, operation)

    def _execute_non_transactional(self, request: Dict[str, Any], operation: str) -> Dict[str, Any]:
        """Execute operation without transaction protection (legacy mode)"""
        if operation == "insert":
            return self._insert(request)
        if operation == "read":
            return self._read(request)
        if operation == "delete":
            return self._delete(request)
        if operation == "update":
            return self._update(request)

        return {
            "success": False,
            "error": f"Unsupported operation: {operation}",
            "supported_operations": ["insert", "read", "update", "delete"],
        }

    def _execute_transactional(self, request: Dict[str, Any], operation: str) -> Dict[str, Any]:
        """Execute operation with automatic transaction protection"""
        # Read operations don't need transactions
        if operation == "read":
            return self._read(request)

        # For INSERT operations, ingest_record has its own transaction mechanism
        # So we don't need the transaction coordinator wrapper
        if operation == "insert":
            return self._insert_transactional(request, None)

        # For write operations (update, delete), use transaction coordinator
        if operation not in ["update", "delete"]:
            return {
                "success": False,
                "error": f"Unsupported operation: {operation}",
                "supported_operations": ["insert", "read", "update", "delete"],
            }

        # Begin transaction
        try:
            tx_id = self.transaction_coordinator.begin_transaction()
            logger.debug("Started transaction %s for operation %s", tx_id, operation)
        except Exception as e:
            logger.error("Failed to begin transaction: %s", e)
            return {
                "success": False,
                "error": f"Failed to begin transaction: {e}"
            }

        try:
            # Execute the operation and collect data for transaction
            if operation == "update":
                result = self._update_transactional(request, tx_id)
            elif operation == "delete":
                result = self._delete_transactional(request, tx_id)
            else:
                raise ValueError(f"Unsupported operation: {operation}")

            if not result.get("success"):
                # Operation failed, abort transaction
                logger.warning("Operation failed, aborting transaction %s", tx_id)
                self.transaction_coordinator.abort(tx_id)
                return result

            # Prepare transaction (2-phase commit phase 1)
            prepare_success, prepare_error = self.transaction_coordinator.prepare(tx_id)
            if not prepare_success:
                logger.error("Transaction prepare failed: %s", prepare_error)
                self.transaction_coordinator.abort(tx_id)
                return {
                    "success": False,
                    "error": f"Transaction prepare failed: {prepare_error}"
                }

            # Commit transaction (2-phase commit phase 2)
            commit_success, commit_error = self.transaction_coordinator.commit(tx_id)
            if not commit_success:
                logger.error("Transaction commit failed: %s", commit_error)
                return {
                    "success": False,
                    "error": f"Transaction commit failed: {commit_error}"
                }

            logger.info("Transaction %s committed successfully", tx_id)
            return result

        except Exception as e:
            logger.error("Error during transactional operation: %s", e, exc_info=True)
            try:
                self.transaction_coordinator.abort(tx_id)
            except:
                pass
            return {
                "success": False,
                "error": f"Operation failed: {e}"
            }

    def _insert_transactional(self, request: Dict[str, Any], tx_id: str) -> Dict[str, Any]:
        """
        Insert operation with transaction support.
        Uses the ingestion pipeline for proper schema handling and field placement.
        """
        data = request.get("data")
        if data is None:
            return {"success": False, "error": "Missing 'data' for insert"}

        # For inserts, we need to use the ingestion callback to handle schema evolution
        # The ingestion pipeline already handles field placement, metadata, etc.
        # We'll wrap each ingest_record call in a transaction

        if isinstance(data, list):
            # Batch insert - each record gets processed through ingestion
            success_count = 0
            for record in data:
                if self.ingest_callback(record):
                    success_count += 1

            logger.info("Transactional batch insert: %s/%s successful", success_count, len(data))
            return {
                "success": success_count == len(data),
                "operation": "insert",
                "inserted": success_count,
                "failed": len(data) - success_count
            }

        # Single insert - process through ingestion pipeline
        success = self.ingest_callback(data)
        logger.info("Transactional insert: success=%s", success)
        return {
            "success": success,
            "operation": "insert",
            "inserted": 1 if success else 0
        }

    def _update_transactional(self, request: Dict[str, Any], tx_id: str) -> Dict[str, Any]:
        """Update operation with transaction support"""
        filters = request.get("filters", {})
        data = request.get("data")

        if data is None:
            return {"success": False, "error": "Missing 'data' for update"}

        # Add update operation (implemented as delete + insert)
        self.transaction_coordinator.add_operation(
            tx_id=tx_id,
            op_type='update',
            backend='both',
            data={'filters': filters, 'new_data': data}
        )

        logger.info("Added update operation to transaction %s", tx_id)
        return {
            "success": True,
            "operation": "update",
            "transaction_id": tx_id
        }

    def _delete_transactional(self, request: Dict[str, Any], tx_id: str) -> Dict[str, Any]:
        """Delete operation with transaction support"""
        filters = request.get("filters", {})

        if not filters and not request.get("allow_all", False):
            return {
                "success": False,
                "error": "Refusing delete without filters. Set allow_all=true to override.",
            }

        # Add delete operation
        self.transaction_coordinator.add_operation(
            tx_id=tx_id,
            op_type='delete',
            backend='both',
            data={'filters': filters}
        )

        logger.info("Added delete operation to transaction %s", tx_id)
        return {
            "success": True,
            "operation": "delete",
            "transaction_id": tx_id
        }

    def _insert(self, request: Dict[str, Any]) -> Dict[str, Any]:
        data = request.get("data")
        if data is None:
            return {"success": False, "error": "Missing 'data' for insert"}

        if isinstance(data, list):
            inserted = 0
            failed = 0
            for record in data:
                if self.ingest_callback(record):
                    inserted += 1
                else:
                    failed += 1
            logger.info("Insert list completed: inserted=%s failed=%s", inserted, failed)
            return {
                "success": failed == 0,
                "operation": "insert",
                "inserted": inserted,
                "failed": failed,
            }

        success = self.ingest_callback(data)
        logger.info("Insert single record success=%s", success)
        return {
            "success": success,
            "operation": "insert",
            "inserted": 1 if success else 0,
        }

    def _filter_conflicting_mongo_paths(self, fields: List[str]) -> List[str]:
        """
        Remove fields that would cause MongoDB path collisions.
        Rules:
        1. If both parent path 'audit' and children 'audit.flags[0]' are present, keep only parent
        2. If multiple indexed paths like 'audit.flags[0]', 'audit.flags[1]' are present, keep only one
        3. Simplified rule: when there's ambiguity, prefer less specific paths
        """
        filtered = []
        
        for field in fields:
            has_conflict = False
            
            # Check if this field conflicts with any other field
            for other_field in fields:
                if field == other_field:
                    continue
                    
                # Case 1: field is a child of other_field (e.g., 'audit.flags[0]' child of 'audit')
                # If parent exists, skip the child
                if field.startswith(other_field + '.') or field.startswith(other_field + '['):
                    has_conflict = True
                    break
                
                # Case 2: field is parent of other_field (e.g., 'audit' parent of 'audit.flags[0]')
                # If parent and child exist, skip the child (other_field)
                # This will be handled in the first case when we process other_field
                
                # Case 3: both are indexed versions of the same parent (e.g., 'audit.flags[0]' and 'audit.flags[1]')
                # Extract parent path
                if '[' in field and ']' in field:
                    field_parent = field[:field.index('[')]
                    if '[' in other_field and ']' in other_field:
                        other_parent = other_field[:other_field.index('[')]
                        if field_parent == other_parent:
                            # Same parent, keep only the lexicographically first one
                            if field > other_field:
                                has_conflict = True
                                break
            
            if not has_conflict:
                filtered.append(field)
        
        # Remove duplicates while preserving order
        seen = set()
        result = []
        for field in filtered:
            if field not in seen:
                seen.add(field)
                result.append(field)
        return result

    def _build_field_plan(self, fields: List[str]) -> Dict[str, Any]:
        sql_root_fields: List[str] = []
        sql_child_entities: Dict[str, str] = {}
        mongo_root_fields: List[str] = []
        mongo_reference_entities: Dict[str, str] = {}

        # Get actual SQL columns to avoid querying non-existent columns
        sql_columns = set(self.sql_manager.get_existing_columns("ingested_records"))

        for field in fields:
            mapping = self.metadata_store.get_field_mapping(field)
            if not mapping:
                continue

            sql_table = mapping.get("sql_table")
            mongo_collection = mapping.get("mongo_collection")

            # Only add to sql_root_fields if the column actually exists in SQL
            if sql_table == "ingested_records" and field in sql_columns:
                sql_root_fields.append(field)
            elif sql_table and sql_table != "ingested_records":
                # Only add child entity if the table exists
                if self.sql_manager.table_exists(sql_table):
                    sql_child_entities[field] = sql_table

            if mongo_collection == "ingested_records":
                mongo_root_fields.append(field)
            elif mongo_collection:
                mongo_reference_entities[field] = mongo_collection

        # Add required fields (only if they exist in SQL)
        for required in ["username", "sys_ingested_at", "t_stamp"]:
            if required in sql_columns and required not in sql_root_fields:
                sql_root_fields.append(required)
            if required not in mongo_root_fields:
                mongo_root_fields.append(required)

        # Filter out conflicting MongoDB paths (e.g., keep 'audit.flags', remove 'audit.flags[0]')
        mongo_root_fields = self._filter_conflicting_mongo_paths(mongo_root_fields)

        return {
            "sql_root_fields": sorted(set(sql_root_fields)),
            "sql_child_entities": sql_child_entities,
            "mongo_root_fields": sorted(set(mongo_root_fields)),
            "mongo_reference_entities": mongo_reference_entities,
        }

    def _split_filters(self, filters: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        sql_filters: Dict[str, Any] = {}
        mongo_filters: Dict[str, Any] = {}

        def _mongo_filter_value(value: Any) -> Any:
            if isinstance(value, dict) and "$starts_with" in value:
                prefix = re.escape(str(value.get("$starts_with", "")))
                return {"$regex": f"^{prefix}", "$options": "i"}
            return value

        for key, value in filters.items():
            mapping = self.metadata_store.get_field_mapping(key)
            if not mapping:
                sql_filters[key] = value
                mongo_filters[key] = _mongo_filter_value(value)
                continue

            if mapping.get("sql_table") == "ingested_records":
                sql_filters[key] = value
            if mapping.get("mongo_collection") == "ingested_records":
                mongo_filters[key] = _mongo_filter_value(value)

        if "username" in filters:
            sql_filters.setdefault("username", filters["username"])
            mongo_filters.setdefault("username", _mongo_filter_value(filters["username"]))

        if "sys_ingested_at" in filters:
            sql_filters.setdefault("sys_ingested_at", filters["sys_ingested_at"])
            mongo_filters.setdefault("sys_ingested_at", _mongo_filter_value(filters["sys_ingested_at"]))

        return {"sql": sql_filters, "mongo": mongo_filters}

    def _read(self, request: Dict[str, Any]) -> Dict[str, Any]:
        fields = request.get("fields")
        if fields:
            # Check if all requested fields exist in metadata
            missing_fields = []
            for field in fields:
                if field in ["username", "sys_ingested_at", "t_stamp"]:
                    continue  # These are always available
                mapping = self.metadata_store.get_field_mapping(field)
                if not mapping:
                    missing_fields.append(field)
            if missing_fields:
                return {"success": False, "error": f"Fields do not exist: {', '.join(missing_fields)}"}
        else:
            fields = list(self.metadata_store.metadata.get("field_mappings", {}).keys())

        filters = request.get("filters", {})
        limit = int(request.get("limit", 100))

        plan = self._build_field_plan(fields)
        filter_plan = self._split_filters(filters)
        logger.debug(
            "Read query plan: sql_root=%s sql_child=%s mongo_root=%s mongo_ref=%s",
            len(plan["sql_root_fields"]),
            len(plan["sql_child_entities"]),
            len(plan["mongo_root_fields"]),
            len(plan["mongo_reference_entities"]),
        )

        sql_rows = self.sql_manager.fetch_records(
            table_name="ingested_records",
            fields=plan["sql_root_fields"],
            filters=filter_plan["sql"],
            limit=limit,
        )

        results_by_key: Dict[str, Dict[str, Any]] = {}
        for row in sql_rows:
            key = row.get("sys_ingested_at")
            if not key:
                continue
            results_by_key[key] = dict(row)

        if not results_by_key:
            mongo_rows = self.mongo_manager.find_records(
                filters=filter_plan["mongo"],
                fields=None,  # No projection — return full document
                collection_name="ingested_records",
                limit=limit,
            )
            for row in mongo_rows:
                key = row.get("sys_ingested_at")
                if not key:
                    continue
                results_by_key[key] = dict(row)


        # OPTIMIZATION: Batch fetch MongoDB root records instead of N+1 queries.
        # Fetch the FULL document (no field projection) so that any field updated
        # via $set (e.g. `country` set after insert) is always included in results.
        if results_by_key:
            keys_list = list(results_by_key.keys())
            mongo_records = self.mongo_manager.find_records(
                filters={"sys_ingested_at": {"$in": keys_list}},
                fields=None,  # No projection — return entire document
                collection_name="ingested_records",
                limit=len(keys_list) * 2,
            )
            # Create a map for O(1) lookup
            mongo_by_key = {rec.get("sys_ingested_at"): rec for rec in mongo_records}

            # Merge MongoDB data into results (SQL data takes precedence for shared fields)
            for key in results_by_key.keys():
                if key in mongo_by_key:
                    # Merge mongo fields in, but don't overwrite SQL fields already present
                    mongo_doc = mongo_by_key[key]
                    for mongo_field, mongo_val in mongo_doc.items():
                        if mongo_field not in results_by_key[key]:
                            results_by_key[key][mongo_field] = mongo_val


        # OPTIMIZATION: Batch fetch child entities per table instead of per-record queries
        for field_name, table_name in plan["sql_child_entities"].items():
            if not results_by_key:
                continue
            
            keys_list = list(results_by_key.keys())
            
            # Build query for all keys at once (using multiple queries if needed for SQL)
            # For now, fetch all children for this table, then filter locally
            all_child_rows = self.sql_manager.fetch_records(
                table_name=table_name,
                fields=None,
                filters={},  # No filter to get all
                limit=len(keys_list) * 1000,
            )
            
            # Group by parent key locally (O(n) for n records)
            children_by_parent = {}
            for row in all_child_rows:
                parent_key = row.get("parent_sys_ingested_at")
                if parent_key in results_by_key:
                    if parent_key not in children_by_parent:
                        children_by_parent[parent_key] = []
                    
                    item = dict(row)
                    item.pop("id", None)
                    item.pop("parent_sys_ingested_at", None)
                    children_by_parent[parent_key].append(item)
            
            # Assign children to records
            for key in results_by_key.keys():
                results_by_key[key][field_name] = children_by_parent.get(key, [])

        # OPTIMIZATION: Batch fetch reference entities per collection instead of per-record queries
        for field_name, collection_name in plan["mongo_reference_entities"].items():
            if not results_by_key:
                continue
            
            keys_list = list(results_by_key.keys())
            
            # Fetch all reference docs for this field across all parent records
            all_reference_docs = self.mongo_manager.find_records(
                filters={
                    "parent_sys_ingested_at": {"$in": keys_list},
                    "entity_path": field_name
                },
                fields=["payload", "item_index", "parent_sys_ingested_at"],
                collection_name=collection_name,
                limit=len(keys_list) * 1000,
            )
            
            # Group by parent key locally (O(n) for n documents)
            refs_by_parent = {}
            for doc in all_reference_docs:
                parent_key = doc.get("parent_sys_ingested_at")
                if parent_key in results_by_key:
                    if parent_key not in refs_by_parent:
                        refs_by_parent[parent_key] = []
                    refs_by_parent[parent_key].append(doc)
            
            # Sort by item_index and extract payloads
            for key in results_by_key.keys():
                if key in refs_by_parent:
                    sorted_docs = sorted(
                        refs_by_parent[key],
                        key=lambda x: x.get("item_index", 0)
                    )
                    payloads = [doc.get("payload") for doc in sorted_docs]
                    results_by_key[key][field_name] = payloads
                else:
                    results_by_key[key][field_name] = []

        records = list(results_by_key.values())
        logger.info("Read completed: records=%s limit=%s (optimized batch queries)", len(records), limit)
        return {
            "success": True,
            "operation": "read",
            "query_plan": {
                "sql_root_fields": plan["sql_root_fields"],
                "sql_filters": filter_plan["sql"],
                "mongo_root_fields": plan["mongo_root_fields"],
                "mongo_filters": filter_plan["mongo"],
                "sql_child_entities": plan["sql_child_entities"],
                "mongo_reference_entities": plan["mongo_reference_entities"],
            },
            "count": len(records),
            "records": records,
        }

    def _delete(self, request: Dict[str, Any]) -> Dict[str, Any]:
        filters = request.get("filters", {})
        entity = request.get("entity")

        if not filters and not request.get("allow_all", False):
            return {
                "success": False,
                "error": "Refusing delete without filters. Set allow_all=true to override.",
            }

        if entity:
            mapping = self.metadata_store.get_field_mapping(entity)
            if not mapping:
                return {"success": False, "error": f"Unknown entity: {entity}"}

            sql_deleted = 0
            mongo_deleted = 0

            if mapping.get("sql_table") and mapping.get("sql_table") != "ingested_records":
                sql_deleted = self.sql_manager.delete_records(mapping["sql_table"], filters)

            if mapping.get("mongo_collection"):
                mongo_deleted = self.mongo_manager.delete_records(filters, mapping["mongo_collection"])

            logger.info(
                "Entity delete completed: entity=%s sql_deleted=%s mongo_deleted=%s",
                entity,
                sql_deleted,
                mongo_deleted,
            )

            return {
                "success": True,
                "operation": "delete",
                "entity": entity,
                "sql_deleted": sql_deleted,
                "mongo_deleted": mongo_deleted,
            }

        root_filter_plan = self._split_filters(filters)

        sql_deleted = self.sql_manager.delete_records("ingested_records", root_filter_plan["sql"])
        mongo_deleted = self.mongo_manager.delete_records(root_filter_plan["mongo"], "ingested_records")
        buffer_deleted = self.mongo_manager.delete_records(root_filter_plan["mongo"], "buffer_records")

        for _, strategy in self.metadata_store.metadata.get("mongo_strategy", {}).get("entities", {}).items():
            collection = strategy.get("collection")
            mode = strategy.get("mode")
            if mode != "reference" or not collection:
                continue
            mongo_deleted += self.mongo_manager.delete_records(root_filter_plan["mongo"], collection)

        logger.info(
            "Root delete completed: sql_deleted=%s mongo_deleted=%s buffer_deleted=%s",
            sql_deleted,
            mongo_deleted,
            buffer_deleted,
        )

        return {
            "success": True,
            "operation": "delete",
            "sql_deleted": sql_deleted,
            "mongo_deleted": mongo_deleted,
            "buffer_deleted": buffer_deleted,
        }

    def _update(self, request: Dict[str, Any]) -> Dict[str, Any]:
        filters = request.get("filters", {})
        data = request.get("data")

        if data is None:
            return {"success": False, "error": "Missing 'data' for update"}

        # Require filters to avoid accidentally updating every record
        if not filters and not request.get("allow_all", False):
            return {
                "success": False,
                "error": "Refusing update without filters. Set allow_all=true to override.",
            }

        # -----------------------------------------------------------------------
        # Route each update field to the correct backend using metadata mappings.
        # A field can go to SQL, MongoDB, or both — determined by its stored mapping.
        # This fixes the bug where MongoDB-only fields (e.g. `country`) were
        # incorrectly rejected because they have no SQL column.
        # -----------------------------------------------------------------------
        sql_updates: Dict[str, Any] = {}
        mongo_updates: Dict[str, Any] = {}

        existing_sql_columns = set(self.sql_manager.get_existing_columns("ingested_records"))

        for field, value in data.items():
            mapping = self.metadata_store.get_field_mapping(field)

            if mapping:
                sql_table = mapping.get("sql_table", "")
                mongo_collection = mapping.get("mongo_collection", "")
                backend = mapping.get("backend", "")

                # Route to SQL root table if the mapping points there
                if sql_table == "ingested_records" or backend in ("SQL", "Both"):
                    safe_field = self.sql_manager._sanitize_identifier(field)
                    if safe_field in existing_sql_columns:
                        sql_updates[safe_field] = value
                    else:
                        # Column missing but mapping says SQL — add it dynamically
                        from type_detector import TypeDetector
                        detected_type = TypeDetector().detect_type(value)
                        self.sql_manager.add_column_if_not_exists(field, detected_type, unique=False)
                        existing_sql_columns = set(
                            self.sql_manager.get_existing_columns("ingested_records")
                        )
                        safe_field = self.sql_manager._sanitize_identifier(field)
                        sql_updates[safe_field] = value

                # Route to MongoDB root collection if the mapping points there
                if mongo_collection == "ingested_records" or backend in ("MongoDB", "Both"):
                    mongo_updates[field] = value

            else:
                # No mapping yet — field may be new.
                # Try SQL if the column already exists; always attempt MongoDB ($set adds new fields).
                safe_field = self.sql_manager._sanitize_identifier(field)
                if safe_field in existing_sql_columns:
                    sql_updates[safe_field] = value
                # MongoDB is schema-less — always include unmapped fields there
                mongo_updates[field] = value

        # Split filters for each backend
        filter_plan = self._split_filters(filters)

        # Execute SQL partial UPDATE (only the specified columns, leaves rest intact)
        sql_updated = 0
        if sql_updates and filter_plan["sql"]:
            sql_updated = self.sql_manager.update_records(
                "ingested_records", filter_plan["sql"], sql_updates
            )
        elif sql_updates:
            logger.warning(
                "Skipping SQL update — no SQL-compatible filters for fields: %s",
                list(sql_updates.keys()),
            )

        # Execute MongoDB partial UPDATE using $set (leaves all other fields intact)
        mongo_updated = 0
        if mongo_updates and filter_plan["mongo"]:
            mongo_updated = self.mongo_manager.update_records(
                filter_plan["mongo"], mongo_updates, "ingested_records"
            )
        elif mongo_updates:
            logger.warning(
                "Skipping MongoDB update — no Mongo-compatible filters for fields: %s",
                list(mongo_updates.keys()),
            )

        success = sql_updated > 0 or mongo_updated > 0

        logger.info(
            "Update completed: sql_updated=%s mongo_updated=%s sql_fields=%s mongo_fields=%s",
            sql_updated,
            mongo_updated,
            list(sql_updates.keys()),
            list(mongo_updates.keys()),
        )
        return {
            "success": success,
            "operation": "update",
            "updated": max(sql_updated, mongo_updated),
        }

