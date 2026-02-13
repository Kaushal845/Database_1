from fastapi import FastAPI
from faker import Faker
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timedelta
import random
import asyncio
import json

random.seed(42)
app = FastAPI()
faker = Faker()

# 1. Unique Field Constraint: Persistent Pool of 1,000 users (The Glue)
USER_POOL = [faker.user_name() for _ in range(1000)]

# Original pool of 50 realistic fields preserved
FIELD_POOL = {
    "name": lambda: faker.name(),
    "age": lambda: random.randint(18, 70),
    "email": lambda: faker.email(),
    "phone": lambda: faker.phone_number(),
    "ip_address": lambda: faker.ipv4(),
    "device_id": lambda: faker.uuid4(),
    "device_model": lambda: random.choice(["iPhone 14", "Pixel 8", "Samsung S23", "OnePlus 12"]),
    "os": lambda: random.choice(["Android", "iOS", "Windows", "Linux", "MacOS"]),
    "app_version": lambda: f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
    "battery": lambda: random.randint(1, 100),
    "charging": lambda: random.choice([True, False]),
    "network": lambda: random.choice(["WiFi", "4G", "5G", "Ethernet", "Offline"]),
    "gps_lat": lambda: float(faker.latitude()),
    "gps_lon": lambda: float(faker.longitude()),
    "altitude": lambda: round(random.uniform(1, 3000), 2),
    "speed": lambda: round(random.uniform(0, 120), 2),
    "direction": lambda: random.choice(["N", "S", "E", "W"]),
    "city": lambda: faker.city(),
    "country": lambda: faker.country(),
    "postal_code": lambda: faker.postcode(),
    "timestamp": lambda: datetime.utcnow().isoformat(),
    "session_id": lambda: faker.uuid4(),
    "steps": lambda: random.randint(0, 12000),
    "heart_rate": lambda: random.randint(60, 180),
    "spo2": lambda: random.randint(90, 100),
    "sleep_hours": lambda: round(random.uniform(3, 9), 1),
    "stress_level": lambda: random.choice(["low", "medium", "high"]),
    "mood": lambda: random.choice(["happy", "sad", "neutral", "angry", "excited"]),
    "weather": lambda: random.choice(["sunny", "rainy", "cloudy", "stormy", "snow"]),
    "temperature_c": lambda: round(random.uniform(-10, 45), 1),
    "humidity": lambda: random.randint(10, 100),
    "air_quality": lambda: random.choice(["good", "moderate", "bad", "hazardous"]),
    "action": lambda: random.choice(["login", "logout", "view", "click", "purchase"]),
    "purchase_value": lambda: round(random.uniform(5, 500), 2),
    "item": lambda: random.choice(["book", "phone", "shoes", "bag", "laptop", None]),
    "payment_status": lambda: random.choice(["success", "failed", "pending"]),
    "subscription": lambda: random.choice(["free", "trial", "basic", "premium"]),
    "language": lambda: faker.language_name(),
    "timezone": lambda: faker.timezone(),
    "cpu_usage": lambda: random.randint(1, 100),
    "ram_usage": lambda: random.randint(1, 100),
    "disk_usage": lambda: random.randint(1, 100),
    "signal_strength": lambda: random.randint(1, 5),
    "error_code": lambda: random.choice([None, 100, 200, 500, 404, 403]),
    "retry_count": lambda: random.randint(0, 5),
    "is_active": lambda: random.choice([True, False]),
    "is_background": lambda: random.choice([True, False]),
    "comment": lambda: faker.sentence(),
    "avatar_url": lambda: faker.image_url(),
    "last_seen": lambda: (datetime.utcnow() - timedelta(minutes=random.randint(1, 300))).isoformat(),
    "friends_count": lambda: random.randint(0, 5000)
}

# --- NEW: BIAS LOGIC (Randomness inside Randomness) ---
# Each field gets a permanent "Appearance Probability" for this server session.
# Some will be > 0.8 (Common/SQL candidates), some < 0.2 (Rare/Mongo candidates).
FIELD_WEIGHTS = {key: random.uniform(0.05, 0.95) for key in FIELD_POOL.keys()}

def get_nested_metadata():
    """Generates consistent nested keys but randomly omits keys AND values."""
    # We define the full potential structure
    full_meta = {
        "sensor_data": {
            "version": "2.1",
            "calibrated": random.choice([True, False]),
            "readings": [random.randint(1, 10) for _ in range(3)]
        },
        "tags": [faker.word() for _ in range(random.randint(1, 3))],
        "is_bot": random.choice([True, False]),
        "internal_id": faker.bothify(text='ID-####-??')
    }
    
    # Heuristic: Randomly drop keys within the nested object (50% chance to drop each key)
    sparse_meta = {k: v for k, v in full_meta.items() if random.random() > 0.5}
    
    # If it's empty, we return None so the field doesn't even appear
    return sparse_meta if sparse_meta else None

def generate_record():
    # Start with the mandatory Username (100% frequency)
    record = {"username": random.choice(USER_POOL)}
    
    # 1. Flat fields: instead of fixed count, we use the pre-defined WEIGHTS
    for key, weight in FIELD_WEIGHTS.items():
        if random.random() < weight:
            record[key] = FIELD_POOL[key]()

    # 2. Nesting: Consistent structure name, but internal data is sparse
    if random.random() > 0.4: # 60% chance to include the metadata block
        meta_content = get_nested_metadata()
        if meta_content:
            record["metadata"] = meta_content
        
    return record

@app.get("/")
async def single_record():
    return generate_record()

@app.get("/record/{count}")
async def stream_records(count: int):
    async def event_generator():
        for _ in range(count):
            await asyncio.sleep(0.01) # Reduced sleep for high-volume 100k tests
            yield {"event": "record", "data": json.dumps(generate_record())}
    return EventSourceResponse(event_generator())