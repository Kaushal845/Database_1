import json
from uuid import uuid4

from ingestion_pipeline import IngestionPipeline


def _make_pipeline(tmp_path):
    metadata_file = tmp_path / "metadata.json"
    sql_db = tmp_path / "ingestion.db"
    pipeline = IngestionPipeline(
        metadata_file=str(metadata_file),
        sql_db=str(sql_db),
        mongo_db="assignment2_test_db",
    )
    return pipeline


def test_schema_registration(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        schema = {
            "required": ["username", "timestamp"],
            "constraints": {"username": {"not_null": True}},
            "entities": {"orders": {"type": "array_of_objects"}},
        }
        result = pipeline.register_schema(schema)

        assert result["schema_version"] == 1
        assert pipeline.metadata_store.get_active_schema() == schema
    finally:
        pipeline.close()


def test_normalization_and_buffer_pipeline(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        record = {
            "username": "user_norm_1",
            "email": "norm1@example.com",
            "volatile_metric": 42,
            "orders": [
                {"order_id": "o-1", "item": "book", "quantity": 1, "price": 10.0},
                {"order_id": "o-2", "item": "bag", "quantity": 2, "price": 40.0},
                {"order_id": "o-3", "item": "phone", "quantity": 1, "price": 300.0},
                {"order_id": "o-4", "item": "shoes", "quantity": 1, "price": 80.0},
            ],
        }

        assert pipeline.ingest_record(record)

        child_tables = pipeline.sql_manager.list_child_tables()
        assert "norm_orders" in child_tables

        normalization_map = pipeline.metadata_store.metadata["normalization"]["child_tables"]
        assert "norm_orders" in normalization_map

        buffer_fields = pipeline.metadata_store.metadata["buffer"]["fields"]
        assert "volatile_metric" in buffer_fields

        mongo_entities = pipeline.metadata_store.metadata["mongo_strategy"]["entities"]
        assert "orders" in mongo_entities
        assert mongo_entities["orders"]["mode"] in {"embed", "reference"}
    finally:
        pipeline.close()


def test_buffer_to_final_transition(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        for index in range(12):
            pipeline.ingest_record(
                {
                    "username": f"user_{index}",
                    "stable_score": index,
                    "timestamp": f"2026-03-22T10:00:{index:02d}",
                }
            )

        mapping = pipeline.metadata_store.get_field_mapping("stable_score")
        decision = pipeline.metadata_store.get_placement_decision("stable_score")

        assert mapping is not None
        assert mapping["status"] == "final"
        assert decision is not None
        assert decision["backend"] in {"SQL", "MongoDB", "Both"}

        buffer_entry = pipeline.metadata_store.metadata["buffer"]["fields"].get("stable_score")
        assert buffer_entry is not None
        assert buffer_entry.get("resolved") is True
    finally:
        pipeline.close()


def test_buffered_scalar_values_are_drained_after_resolution(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        user_prefix = f"buffer_scalar_{uuid4().hex[:8]}_"

        for index in range(2):
            assert pipeline.ingest_record(
                {
                    "username": f"{user_prefix}{index}",
                    "stable_score": index,
                }
            )

        early_buffer_docs = pipeline.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name="buffer_records",
            limit=200,
        )
        early_field_hits = [
            doc
            for doc in early_buffer_docs
            if doc.get("username", "").startswith(user_prefix)
            and "stable_score" in doc.get("fields", {})
        ]
        assert len(early_field_hits) == 2

        for index in range(2, 9):
            assert pipeline.ingest_record(
                {
                    "username": f"{user_prefix}{index}",
                    "stable_score": index,
                }
            )

        final_buffer_docs = pipeline.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name="buffer_records",
            limit=200,
        )
        remaining_field_hits = [
            doc
            for doc in final_buffer_docs
            if doc.get("username", "").startswith(user_prefix)
            and "stable_score" in doc.get("fields", {})
        ]
        assert len(remaining_field_hits) == 0

        decision = pipeline.metadata_store.get_placement_decision("stable_score")
        assert decision is not None
        backend = decision["backend"]

        columns = [
            row[1]
            for row in pipeline.sql_manager.connection.execute("PRAGMA table_info(ingested_records)").fetchall()
        ]
        sql_rows_with_value = 0
        if "stable_score" in columns:
            sql_rows_with_value = pipeline.sql_manager.connection.execute(
                """
                SELECT COUNT(*) FROM ingested_records
                WHERE username LIKE ? AND stable_score IS NOT NULL
                """,
                (f"{user_prefix}%",),
            ).fetchone()[0]

        mongo_docs = pipeline.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name="ingested_records",
            limit=500,
        )
        mongo_rows_with_value = len(
            [
                doc
                for doc in mongo_docs
                if doc.get("username", "").startswith(user_prefix) and "stable_score" in doc
            ]
        )

        if backend == "SQL":
            assert sql_rows_with_value == 9
        elif backend == "MongoDB":
            assert mongo_rows_with_value == 9
        else:
            assert sql_rows_with_value == 9
            assert mongo_rows_with_value == 9
    finally:
        pipeline.close()


def test_buffered_nested_values_are_drained_after_resolution(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        user_prefix = f"buffer_nested_{uuid4().hex[:8]}_"

        for index in range(12):
            assert pipeline.ingest_record(
                {
                    "username": f"{user_prefix}{index}",
                    "preferences": {
                        "timezone": "UTC",
                        "language": "en",
                    },
                }
            )

        mapping = pipeline.metadata_store.get_field_mapping("preferences")
        assert mapping is not None
        assert mapping["status"] == "final"

        buffer_docs = pipeline.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name="buffer_records",
            limit=200,
        )
        nested_buffer_hits = [
            doc
            for doc in buffer_docs
            if doc.get("username", "").startswith(user_prefix)
            and "preferences" in doc.get("fields", {})
        ]
        assert len(nested_buffer_hits) == 0

        mongo_docs = pipeline.mongo_manager.find_records(
            filters={},
            fields=None,
            collection_name="ingested_records",
            limit=500,
        )
        nested_docs_with_preferences = [
            doc
            for doc in mongo_docs
            if doc.get("username", "").startswith(user_prefix) and "preferences" in doc
        ]
        assert len(nested_docs_with_preferences) == 12
    finally:
        pipeline.close()


def test_metadata_driven_crud_cycle(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        for index in range(12):
            pipeline.ingest_record(
                {
                    "username": f"crud_user_{index}",
                    "email": f"crud{index}@example.com",
                    "orders": [
                        {
                            "order_id": f"ord-{index}",
                            "item": "book",
                            "quantity": 1,
                            "price": 15.0,
                        }
                    ],
                }
            )

        insert_result = pipeline.execute_crud(
            {
                "operation": "insert",
                "data": {
                    "username": "crud_target",
                    "email": "before@example.com",
                    "orders": [{"order_id": "ord-target", "item": "bag", "quantity": 1, "price": 25.0}],
                },
            }
        )
        assert insert_result["success"]

        read_result = pipeline.execute_crud(
            {
                "operation": "read",
                "fields": ["username", "email", "orders"],
                "filters": {"username": "crud_target"},
            }
        )
        assert read_result["success"]
        assert read_result["count"] >= 1

        update_result = pipeline.execute_crud(
            {
                "operation": "update",
                "filters": {"username": "crud_target"},
                "data": {
                    "username": "crud_target",
                    "email": "after@example.com",
                    "orders": [{"order_id": "ord-target2", "item": "phone", "quantity": 1, "price": 300.0}],
                },
            }
        )
        assert update_result["success"]

        after_read = pipeline.execute_crud(
            {
                "operation": "read",
                "fields": ["username", "email"],
                "filters": {"username": "crud_target"},
            }
        )
        assert after_read["success"]
        emails = [row.get("email") for row in after_read["records"]]
        assert "after@example.com" in emails

        delete_result = pipeline.execute_crud(
            {
                "operation": "delete",
                "filters": {"username": "crud_target"},
            }
        )
        assert delete_result["success"]

        final_read = pipeline.execute_crud(
            {
                "operation": "read",
                "fields": ["username", "email"],
                "filters": {"username": "crud_target"},
            }
        )
        assert final_read["success"]
        assert final_read["count"] == 0
    finally:
        pipeline.close()


def test_primitive_array_normalization_creates_value_table(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        assert pipeline.ingest_record(
            {
                "username": "primitive_array_user",
                "tags": ["new", "sale", "popular"],
            }
        )

        child_tables = pipeline.sql_manager.list_child_tables()
        assert "norm_tags" in child_tables

        parent_key = pipeline.sql_manager.connection.execute(
            "SELECT sys_ingested_at FROM ingested_records WHERE username = ?",
            ("primitive_array_user",),
        ).fetchone()[0]

        rows = pipeline.sql_manager.fetch_records(
            table_name="norm_tags",
            fields=["value", "item_index"],
            filters={"parent_sys_ingested_at": parent_key},
            limit=20,
        )
        values = [row["value"] for row in sorted(rows, key=lambda item: item["item_index"])]
        assert values == ["new", "sale", "popular"]
    finally:
        pipeline.close()


def test_repeated_scalar_group_normalized_into_child_table(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        assert pipeline.ingest_record(
            {
                "username": "scalar_group_user",
                "phone1": "111-111",
                "phone2": "222-222",
                "phone3": "333-333",
            }
        )

        child_tables = pipeline.sql_manager.list_child_tables()
        assert "norm_phone" in child_tables

        parent_key = pipeline.sql_manager.connection.execute(
            "SELECT sys_ingested_at FROM ingested_records WHERE username = ?",
            ("scalar_group_user",),
        ).fetchone()[0]

        rows = pipeline.sql_manager.fetch_records(
            table_name="norm_phone",
            fields=["value", "item_index"],
            filters={"parent_sys_ingested_at": parent_key},
            limit=20,
        )
        values = [row["value"] for row in sorted(rows, key=lambda item: item["item_index"])]
        assert values == ["111-111", "222-222", "333-333"]

        root_columns = [
            row[1]
            for row in pipeline.sql_manager.connection.execute("PRAGMA table_info(ingested_records)").fetchall()
        ]
        assert "phone1" not in root_columns
        assert "phone2" not in root_columns
        assert "phone3" not in root_columns
    finally:
        pipeline.close()


def test_mongo_strategy_uses_schema_hints_for_reference(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        schema = {
            "entities": {
                "activity_log": {
                    "type": "array_of_objects",
                    "frequently_updated": True,
                    "unbounded": True,
                }
            }
        }
        pipeline.register_schema(schema)

        for index in range(12):
            assert pipeline.ingest_record(
                {
                    "username": f"hint_user_{index}",
                    "activity_log": [
                        {"event": "login", "at": f"2026-03-22T10:00:{index:02d}"},
                        {"event": "click", "at": f"2026-03-22T10:01:{index:02d}"},
                    ],
                }
            )

        mongo_entities = pipeline.metadata_store.metadata["mongo_strategy"]["entities"]
        assert "activity_log" in mongo_entities
        assert mongo_entities["activity_log"]["mode"] == "reference"
        assert isinstance(mongo_entities["activity_log"].get("decision_score"), int)
        assert "schema_hint_unbounded" in mongo_entities["activity_log"].get("decision_reasons", [])
        assert mongo_entities["activity_log"].get("reference_threshold") == 2
    finally:
        pipeline.close()


def test_nested_fields_skip_buffer_warmup_and_route_to_mongo(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        assert pipeline.ingest_record(
            {
                "username": "nested_warmup_user",
                "orders": [
                    {"order_id": "o-100", "item": "book", "quantity": 1, "price": 20.0},
                    {"order_id": "o-101", "item": "bag", "quantity": 1, "price": 40.0},
                ],
                "devices": [
                    {"device_id": "d-1", "model": "Pixel", "os": "Android", "battery": 80},
                ],
            }
        )

        buffer_docs = pipeline.mongo_manager.find_records(
            filters={"username": "nested_warmup_user"},
            fields=None,
            collection_name="buffer_records",
            limit=10,
        )
        assert len(buffer_docs) == 0

        orders_decision = pipeline.metadata_store.get_placement_decision("orders")
        devices_decision = pipeline.metadata_store.get_placement_decision("devices")
        assert orders_decision is not None and orders_decision["backend"] == "MongoDB"
        assert devices_decision is not None and devices_decision["backend"] == "MongoDB"
    finally:
        pipeline.close()
