from fastapi import FastAPI
from faker import Faker
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import random
import asyncio
import json

random.seed(42)
app = FastAPI()
faker = Faker()

# 1. Unique Field Constraint: Persistent Pool of 1,000 users (The Glue)
USER_POOL = [faker.user_name() for _ in range(1000)]

DEFAULT_SCHEMA = {
    "required": ["username", "timestamp", "session_id"],
    "constraints": {
        "username": {"not_null": True},
        "session_id": {"unique": True},
        "timestamp": {"not_null": True},
        "email": {"format": "email"}
    },
    "entities": {
        "orders": {
            "type": "array_of_objects",
            "relation": "one_to_many"
        },
        "comments": {
            "type": "array_of_objects",
            "relation": "one_to_many"
        },
        "devices": {
            "type": "array_of_objects",
            "relation": "one_to_many"
        },
        "profile": {
            "type": "object",
            "relation": "embedded"
        }
    }
}

FIELD_POOL = {
    "name": lambda: faker.name(),
    "age": lambda: random.randint(18, 70),
    "email": lambda: faker.email(),
    "phone": lambda: faker.phone_number(),
    "ip_address": lambda: faker.ipv4(),
    "city": lambda: faker.city(),
    "country": lambda: faker.country(),
    "postal_code": lambda: faker.postcode(),
    "session_id": lambda: faker.uuid4(),
    "event_type": lambda: random.choice(["login", "purchase", "comment", "logout", "view"]),
    "device_model": lambda: random.choice(["iPhone 14", "Pixel 8", "Samsung S23", "OnePlus 12"]),
    "app_version": lambda: f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
    "subscription": lambda: random.choice(["free", "trial", "basic", "premium"]),
    "is_active": lambda: random.choice([True, False]),
    "retry_count": lambda: random.randint(0, 5),
    "timestamp": lambda: datetime.now(timezone.utc).isoformat(),
    "last_seen": lambda: (datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 300))).isoformat(),
}

FIELD_WEIGHTS = {key: random.uniform(0.05, 0.95) for key in FIELD_POOL.keys()}

def get_profile() -> Dict[str, Any]:
    return {
        "language": faker.language_name(),
        "timezone": faker.timezone(),
        "preferences": {
            "notifications": random.choice([True, False]),
            "theme": random.choice(["light", "dark", "system"]),
            "currency": random.choice(["USD", "EUR", "INR", "GBP"]),
        },
        "security": {
            "mfa_enabled": random.choice([True, False]),
            "last_password_change": faker.date_time_this_year().isoformat(),
        },
    }


def get_orders() -> List[Dict[str, Any]]:
    count = random.randint(1, 6)
    rows = []
    for _ in range(count):
        rows.append(
            {
                "order_id": faker.uuid4(),
                "item": random.choice(["book", "phone", "shoes", "bag", "laptop"]),
                "quantity": random.randint(1, 4),
                "price": round(random.uniform(8, 600), 2),
                "status": random.choice(["created", "paid", "shipped", "delivered"]),
                "ordered_at": faker.date_time_this_year().isoformat(),
            }
        )
    return rows


def get_comments() -> List[Dict[str, Any]]:
    count = random.randint(0, 4)
    rows = []
    for _ in range(count):
        rows.append(
            {
                "comment_id": faker.uuid4(),
                "text": faker.sentence(),
                "sentiment": random.choice(["positive", "neutral", "negative"]),
                "created_at": faker.date_time_this_year().isoformat(),
            }
        )
    return rows


def get_devices() -> List[Dict[str, Any]]:
    count = random.randint(1, 5)
    rows = []
    for _ in range(count):
        rows.append(
            {
                "device_id": faker.uuid4(),
                "model": random.choice(["iPhone 14", "Pixel 8", "Samsung S23", "OnePlus 12"]),
                "os": random.choice(["Android", "iOS", "Windows", "Linux", "MacOS"]),
                "battery": random.randint(1, 100),
                "is_primary": random.choice([True, False]),
            }
        )
    return rows


def generate_record() -> Dict[str, Any]:
    record = {"username": random.choice(USER_POOL)}

    for key, weight in FIELD_WEIGHTS.items():
        if random.random() < weight:
            record[key] = FIELD_POOL[key]()

    if random.random() < 0.82:
        record["profile"] = get_profile()

    if random.random() < 0.78:
        record["orders"] = get_orders()

    if random.random() < 0.68:
        comments = get_comments()
        if comments:
            record["comments"] = comments

    if random.random() < 0.62:
        record["devices"] = get_devices()

    if random.random() < 0.35:
        record["audit"] = {
            "source": random.choice(["mobile_app", "web", "batch", "internal"]),
            "risk_score": random.randint(1, 100),
            "flags": [faker.word() for _ in range(random.randint(1, 3))],
        }
        
    return record

@app.get("/")
async def single_record():
    return generate_record()


@app.get("/schema")
async def schema_definition():
    return DEFAULT_SCHEMA


@app.get("/health")
async def health():
    return {"status": "ok", "service": "assignment-2-generator"}

@app.get("/record/{count}")
async def stream_records(count: int):
    async def event_generator():
        for _ in range(count):
            await asyncio.sleep(0.01)
            yield {"event": "record", "data": json.dumps(generate_record())}
    return EventSourceResponse(event_generator())
