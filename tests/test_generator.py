from fastapi.testclient import TestClient

from main import app, generate_record


def test_health_and_schema_endpoints():
    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    schema = client.get("/schema")
    assert schema.status_code == 200
    payload = schema.json()
    assert "required" in payload
    assert "entities" in payload
    assert "orders" in payload["entities"]


def test_generated_records_include_assignment2_structures():
    saw_orders = False
    saw_profile = False
    saw_devices = False

    for _ in range(80):
        record = generate_record()
        if "orders" in record and isinstance(record["orders"], list):
            saw_orders = True
        if "profile" in record and isinstance(record["profile"], dict):
            saw_profile = True
        if "devices" in record and isinstance(record["devices"], list):
            saw_devices = True

        if saw_orders and saw_profile and saw_devices:
            break

    assert saw_orders
    assert saw_profile
    assert saw_devices
