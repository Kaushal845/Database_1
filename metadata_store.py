"""
Metadata Store - Persists schema, placement, normalization, and query metadata.
"""
import json
import os
import threading
import tempfile
import shutil
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from logging_utils import get_logger


logger = get_logger("metadata")


class MetadataStore:
    """
    Persistent metadata manager for the hybrid SQL/MongoDB framework.

    Assignment-2 additions:
    - Schema registry with version history
    - Buffer tracking for undecided fields
    - SQL normalization table mapping
    - MongoDB document strategy mapping (embed/reference)
    - Field-level routing map for query generation
    """

    def __init__(self, storage_file: str = "metadata_store.json", auto_save: bool = True):
        self.storage_file = storage_file
        self.auto_save = auto_save
        self.lock = threading.Lock()
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata from disk and upgrade to the latest shape if required."""
        if not os.path.exists(self.storage_file):
            return self._initialize_metadata()

        try:
            with open(self.storage_file, "r", encoding="utf-8") as file:
                existing = json.load(file)
        except Exception as error:
            logger.error("Error loading metadata from %s: %s", self.storage_file, error)
            logger.info("Initializing fresh metadata")
            return self._initialize_metadata()

        return self._upgrade_metadata(existing)

    def _initialize_metadata(self) -> Dict[str, Any]:
        """Create a fully initialized metadata document."""
        now = datetime.now(timezone.utc).isoformat()
        return {
            "version": 2,
            "fields": {},
            "placement_decisions": {},
            "current_placement": {},
            "field_mappings": {},
            "schema_registry": {
                "active_version": 0,
                "active_schema": {},
                "versions": []
            },
            "buffer": {
                "fields": {}
            },
            "normalization": {
                "root_table": "ingested_records",
                "child_tables": {}
            },
            "mongo_strategy": {
                "root_collection": "ingested_records",
                "entities": {}
            },
            "quarantined_fields": {},
            "total_records": 0,
            "last_updated": now,
            "session_start": now
        }

    def _upgrade_metadata(self, existing: Dict[str, Any]) -> Dict[str, Any]:
        """Backfill missing keys so old metadata files remain compatible."""
        base = self._initialize_metadata()

        for key in [
            "fields",
            "placement_decisions",
            "current_placement",
            "total_records",
            "last_updated",
            "session_start",
            "quarantined_fields",
        ]:
            if key in existing:
                base[key] = existing[key]

        if "field_mappings" in existing:
            base["field_mappings"] = existing["field_mappings"]

        if "schema_registry" in existing:
            base["schema_registry"].update(existing["schema_registry"])

        if "buffer" in existing:
            base["buffer"].update(existing["buffer"])

        if "normalization" in existing:
            base["normalization"].update(existing["normalization"])

        if "mongo_strategy" in existing:
            base["mongo_strategy"].update(existing["mongo_strategy"])

        base["version"] = 2
        return base

    def save(self):
        """Persist metadata to disk using atomic writes."""
        with self.lock:
            self.metadata["last_updated"] = datetime.now(timezone.utc).isoformat()
            try:
                # Use atomic writes: write to temp file, then move
                storage_dir = os.path.dirname(self.storage_file) or "."
                with tempfile.NamedTemporaryFile(
                    mode="w", dir=storage_dir, delete=False, encoding="utf-8"
                ) as temp_file:
                    json.dump(self.metadata, temp_file, indent=2)
                    temp_path = temp_file.name
                
                # Atomic replace
                shutil.move(temp_path, self.storage_file)
                logger.debug("Metadata saved successfully to %s", self.storage_file)
            except Exception as error:
                logger.error("Error saving metadata to %s: %s", self.storage_file, error)
                # Clean up temp file if it still exists
                try:
                    if 'temp_path' in locals() and os.path.exists(temp_path):
                        os.unlink(temp_path)
                except Exception:
                    pass

    def increment_total_records(self):
        """Increment processed record count."""
        with self.lock:
            self.metadata["total_records"] += 1
        if self.auto_save:
            self.save()

    def register_schema(self, schema: Dict[str, Any]) -> int:
        """Register a new schema version and set it as active."""
        with self.lock:
            next_version = self.metadata["schema_registry"]["active_version"] + 1
            self.metadata["schema_registry"]["active_version"] = next_version
            self.metadata["schema_registry"]["active_schema"] = schema
            self.metadata["schema_registry"]["versions"].append(
                {
                    "version": next_version,
                    "registered_at": datetime.now(timezone.utc).isoformat(),
                    "schema": schema,
                }
            )

        if self.auto_save:
            self.save()
        return next_version

    def get_active_schema(self) -> Dict[str, Any]:
        """Return the currently active schema definition."""
        return self.metadata.get("schema_registry", {}).get("active_schema", {})

    def update_field_stats(self, normalized_key: str, data_type: str, value: Any):
        """Update appearance and type statistics for a field."""
        with self.lock:
            if normalized_key not in self.metadata["fields"]:
                self.metadata["fields"][normalized_key] = {
                    "appearances": 0,
                    "type_counts": {},
                    "first_seen": datetime.now(timezone.utc).isoformat(),
                    "last_seen": datetime.now(timezone.utc).isoformat(),
                    "sample_values": [],
                }

            field_data = self.metadata["fields"][normalized_key]
            field_data["appearances"] += 1
            field_data["last_seen"] = datetime.now(timezone.utc).isoformat()

            if data_type not in field_data["type_counts"]:
                field_data["type_counts"][data_type] = 0
            field_data["type_counts"][data_type] += 1

            value_sample = str(value)[:120]
            if value_sample not in field_data["sample_values"] and len(field_data["sample_values"]) < 8:
                field_data["sample_values"].append(value_sample)

        if self.auto_save:
            self.save()

    def add_buffer_observation(self, field_name: str, value: Any):
        """Track values for fields currently routed to Buffer."""
        with self.lock:
            buffer_fields = self.metadata.setdefault("buffer", {}).setdefault("fields", {})
            if field_name not in buffer_fields:
                buffer_fields[field_name] = {
                    "observations": 0,
                    "sample_values": [],
                    "first_seen": datetime.now(timezone.utc).isoformat(),
                    "last_seen": datetime.now(timezone.utc).isoformat(),
                    "resolved": False,
                }

            buffer_entry = buffer_fields[field_name]
            buffer_entry["observations"] += 1
            buffer_entry["last_seen"] = datetime.now(timezone.utc).isoformat()

            value_sample = str(value)[:120]
            if value_sample not in buffer_entry["sample_values"] and len(buffer_entry["sample_values"]) < 8:
                buffer_entry["sample_values"].append(value_sample)

        if self.auto_save:
            self.save()

    def resolve_buffer_field(self, field_name: str, backend: str):
        """Mark a buffered field as resolved and routed to final backend."""
        with self.lock:
            buffer_fields = self.metadata.setdefault("buffer", {}).setdefault("fields", {})
            if field_name in buffer_fields:
                buffer_fields[field_name]["resolved"] = True
                buffer_fields[field_name]["resolved_backend"] = backend
                buffer_fields[field_name]["resolved_at"] = datetime.now(timezone.utc).isoformat()

        if self.auto_save:
            self.save()

    def set_placement_decision(self, normalized_key: str, backend: str, reason: str):
        """Store placement decision for a field and update current placement."""
        with self.lock:
            self.metadata["placement_decisions"][normalized_key] = {
                "backend": backend,
                "reason": reason,
                "decided_at": datetime.now(timezone.utc).isoformat(),
            }
            self.metadata["current_placement"][normalized_key] = backend

            if backend != "Buffer":
                buffer_fields = self.metadata.setdefault("buffer", {}).setdefault("fields", {})
                if normalized_key in buffer_fields:
                    buffer_fields[normalized_key]["resolved"] = True
                    buffer_fields[normalized_key]["resolved_backend"] = backend
                    buffer_fields[normalized_key]["resolved_at"] = datetime.now(timezone.utc).isoformat()

        if self.auto_save:
            self.save()

    def set_field_mapping(
        self,
        field_name: str,
        backend: str,
        sql_table: Optional[str] = None,
        mongo_collection: Optional[str] = None,
        status: str = "final",
    ):
        """Maintain routing metadata used by the query generator."""
        with self.lock:
            self.metadata["field_mappings"][field_name] = {
                "backend": backend,
                "sql_table": sql_table,
                "mongo_collection": mongo_collection,
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }

        if self.auto_save:
            self.save()

    def get_field_mapping(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get field mapping metadata for a field."""
        return self.metadata.get("field_mappings", {}).get(field_name)

    def register_normalized_table(
        self,
        table_name: str,
        entity_path: str,
        columns: List[str],
        foreign_key: str = "parent_sys_ingested_at",
    ):
        """Register SQL normalization output table metadata."""
        with self.lock:
            self.metadata.setdefault("normalization", {}).setdefault("child_tables", {})[table_name] = {
                "entity_path": entity_path,
                "columns": columns,
                "foreign_key": foreign_key,
                "registered_at": datetime.now(timezone.utc).isoformat(),
            }

        if self.auto_save:
            self.save()

    def register_mongo_entity(self, entity_path: str, mode: str, collection: str):
        """Register MongoDB embed/reference strategy for nested entities."""
        with self.lock:
            self.metadata.setdefault("mongo_strategy", {}).setdefault("entities", {})[entity_path] = {
                "mode": mode,
                "collection": collection,
                "registered_at": datetime.now(timezone.utc).isoformat(),
            }

        if self.auto_save:
            self.save()

    def register_mongo_entity_with_decision(
        self,
        entity_path: str,
        mode: str,
        collection: str,
        decision_score: int,
        decision_reasons: List[str],
        reference_threshold: int,
        schema_hints: Optional[Dict[str, Any]] = None,
    ):
        """Register MongoDB strategy plus decision telemetry for reporting/debugging."""
        with self.lock:
            self.metadata.setdefault("mongo_strategy", {}).setdefault("entities", {})[entity_path] = {
                "mode": mode,
                "collection": collection,
                "decision_score": decision_score,
                "reference_threshold": reference_threshold,
                "decision_reasons": decision_reasons,
                "schema_hints": schema_hints or {},
                "registered_at": datetime.now(timezone.utc).isoformat(),
            }

        if self.auto_save:
            self.save()

    def get_placement_decision(self, normalized_key: str) -> Optional[Dict[str, str]]:
        """Get placement decision for a field."""
        return self.metadata.get("placement_decisions", {}).get(normalized_key)

    def get_field_frequency(self, normalized_key: str) -> float:
        """Calculate field appearance frequency as percentage."""
        total_records = self.metadata.get("total_records", 0)
        if total_records == 0:
            return 0.0

        field_data = self.metadata.get("fields", {}).get(normalized_key)
        if not field_data:
            return 0.0

        return (field_data["appearances"] / total_records) * 100.0

    def get_field_type_stability(self, normalized_key: str) -> tuple:
        """Return dominant type and stability percentage for a field."""
        field_data = self.metadata.get("fields", {}).get(normalized_key)
        if not field_data or not field_data.get("type_counts"):
            return ("unknown", 0.0)

        total_appearances = sum(field_data["type_counts"].values())
        dominant_type = max(field_data["type_counts"], key=field_data["type_counts"].get)
        dominant_count = field_data["type_counts"][dominant_type]
        stability = (dominant_count / total_appearances) * 100.0

        return (dominant_type, stability)

    def get_field_stats(self, normalized_key: str) -> Dict[str, Any]:
        """Get comprehensive statistics for a field."""
        field_data = self.metadata.get("fields", {}).get(normalized_key)
        if not field_data:
            return {
                "frequency": 0.0,
                "type_stability": 0.0,
                "drift_score": 0.0,
                "appearances": 0,
                "null_ratio": 1.0,
                "dominant_type": "unknown",
                "type_counts": {},
            }

        frequency = self.get_field_frequency(normalized_key)
        dominant_type, type_stability = self.get_field_type_stability(normalized_key)
        drift_score = (100.0 - type_stability) / 100.0

        total_records = self.metadata.get("total_records", 0)
        appearances = field_data["appearances"]
        null_ratio = 1.0 - (appearances / total_records) if total_records > 0 else 1.0

        return {
            "frequency": frequency,
            "type_stability": type_stability,
            "drift_score": drift_score,
            "appearances": appearances,
            "null_ratio": null_ratio,
            "dominant_type": dominant_type,
            "type_counts": field_data.get("type_counts", {}),
        }

    def mark_quarantined(self, normalized_key: str, drift_score: float):
        """Mark a field as quarantined due to severe drift."""
        with self.lock:
            self.metadata.setdefault("quarantined_fields", {})[normalized_key] = {
                "drift_score": drift_score,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "reason": f"Severe type drift detected (score: {drift_score:.2f})",
            }

        if self.auto_save:
            self.save()

    def is_quarantined(self, normalized_key: str) -> bool:
        """Return True if field has been quarantined."""
        return normalized_key in self.metadata.get("quarantined_fields", {})

    def get_field_placement(self, normalized_key: str) -> str:
        """Get current field placement."""
        return self.metadata.get("current_placement", {}).get(normalized_key, "unknown")

    def get_fields_by_placement(self, backend: str) -> List[str]:
        """Get all fields currently assigned to one backend."""
        return [
            field_name
            for field_name, placement in self.metadata.get("current_placement", {}).items()
            if placement == backend
        ]

    def get_all_fields(self) -> List[str]:
        """Get all tracked fields."""
        return list(self.metadata.get("fields", {}).keys())

    def get_statistics(self) -> Dict[str, Any]:
        """Get high-level metadata stats."""
        return {
            "total_records": self.metadata.get("total_records", 0),
            "unique_fields": len(self.metadata.get("fields", {})),
            "placement_decisions": len(self.metadata.get("placement_decisions", {})),
            "buffer_fields": len(self.metadata.get("buffer", {}).get("fields", {})),
            "schema_versions": len(self.metadata.get("schema_registry", {}).get("versions", [])),
            "normalized_tables": len(self.metadata.get("normalization", {}).get("child_tables", {})),
            "session_start": self.metadata.get("session_start"),
            "last_updated": self.metadata.get("last_updated"),
        }

    def get_placement_summary(self) -> Dict[str, Any]:
        """Get placement summary across SQL, MongoDB, Buffer, and Both."""
        current_placement = self.metadata.get("current_placement", {})
        sql_fields = [field for field, place in current_placement.items() if place == "SQL"]
        mongo_fields = [field for field, place in current_placement.items() if place == "MongoDB"]
        both_fields = [field for field, place in current_placement.items() if place == "Both"]
        buffer_fields = [field for field, place in current_placement.items() if place == "Buffer"]

        return {
            "sql_field_count": len(sql_fields),
            "mongodb_field_count": len(mongo_fields),
            "both_field_count": len(both_fields),
            "buffer_field_count": len(buffer_fields),
            "sql_fields": sql_fields,
            "mongodb_fields": mongo_fields,
            "both_fields": both_fields,
            "buffer_fields": buffer_fields,
        }

    def get_unmapped_fields(self) -> List[str]:
        """
        Return list of fields that have been discovered but have NO placement decision.
        These are fields in metadata['fields'] but NOT in metadata['current_placement'].
        """
        all_fields = set(self.metadata.get("fields", {}).keys())
        mapped_fields = set(self.metadata.get("current_placement", {}).keys())
        unmapped = list(all_fields - mapped_fields)
        logger.info(
            "Unmapped fields report: %d total fields, %d mapped, %d unmapped",
            len(all_fields),
            len(mapped_fields),
            len(unmapped),
        )
        return sorted(unmapped)

    def finalize_unmapped_to_mongodb(self) -> Dict[str, Any]:
        """
        Automatically route all unmapped fields to MongoDB.
        Called at program finalization to ensure NO field is left without a backend assignment.
        
        Returns: Dictionary with finalization statistics
        """
        unmapped_fields = self.get_unmapped_fields()
        finalization_stats = {
            "unmapped_count": len(unmapped_fields),
            "fields_finalized_to_mongodb": 0,
            "finalized_fields": [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if not unmapped_fields:
            logger.info("No unmapped fields to finalize - all fields have placement decisions")
            return finalization_stats

        logger.info(
            "Finalizing %d unmapped fields to MongoDB automatically",
            len(unmapped_fields),
        )

        with self.lock:
            for field_name in unmapped_fields:
                try:
                    # Set placement decision
                    self.metadata["placement_decisions"][field_name] = {
                        "backend": "MongoDB",
                        "reason": "Auto-finalized to MongoDB at program completion (unmapped field)",
                        "decided_at": datetime.now(timezone.utc).isoformat(),
                    }

                    # Set current placement
                    self.metadata["current_placement"][field_name] = "MongoDB"

                    # Set field mapping with 'auto_finalized' status
                    self.metadata["field_mappings"][field_name] = {
                        "backend": "MongoDB",
                        "sql_table": None,
                        "mongo_collection": "ingested_records",  # Root collection by default
                        "status": "auto_finalized",
                        "finalized_at": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }

                    # Remove from Buffer if present
                    buffer_fields = self.metadata.get("buffer", {}).get("fields", {})
                    if field_name in buffer_fields:
                        buffer_fields[field_name]["resolved"] = True
                        buffer_fields[field_name]["resolved_backend"] = "MongoDB"
                        buffer_fields[field_name]["resolved_at"] = datetime.now(timezone.utc).isoformat()

                    finalization_stats["fields_finalized_to_mongodb"] += 1
                    finalization_stats["finalized_fields"].append(field_name)
                    logger.debug("Auto-finalized field '%s' to MongoDB", field_name)

                except Exception as e:
                    logger.error("Error finalizing field '%s': %s", field_name, e)

        if self.auto_save:
            self.save()

        logger.info(
            "Finalization complete: %d fields moved to MongoDB",
            finalization_stats["fields_finalized_to_mongodb"],
        )

        return finalization_stats

