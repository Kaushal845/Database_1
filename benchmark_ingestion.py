"""Run one-batch ingestion benchmarks across multiple batch sizes."""

from __future__ import annotations

import contextlib
import io
import json
import time
from pathlib import Path

import requests
from pymongo import MongoClient

from data_consumer import DataConsumer
from ingestion_pipeline import IngestionPipeline
from logging_utils import setup_logging


def _drop_database(db_name: str, mongo_uri: str = "mongodb://localhost:27017/") -> None:
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        client.drop_database(db_name)
        client.close()
    except Exception:
        # Benchmark should still run with in-memory fallback if Mongo is unavailable.
        return


def run_benchmark(batch_sizes: list[int]) -> dict:
    setup_logging("WARNING")
    requests.get("http://127.0.0.1:8000/health", timeout=5).raise_for_status()

    results = []
    for size in batch_sizes:
        metadata_path = Path("docs") / f"benchmark_metadata_{size}.json"
        sql_path = Path("docs") / f"benchmark_ingestion_{size}.db"
        mongo_db = f"benchmark_ingestion_{size}"

        if metadata_path.exists():
            metadata_path.unlink()
        if sql_path.exists():
            sql_path.unlink()
        _drop_database(mongo_db)

        pipeline = IngestionPipeline(
            metadata_file=str(metadata_path),
            sql_db=str(sql_path),
            mongo_db=mongo_db,
        )

        consumer = DataConsumer(api_url="http://127.0.0.1:8000", pipeline=pipeline)
        start = time.perf_counter()
        consumer.consume_continuous(
            batch_size=size,
            total_batches=1,
            delay=0.0,
            close_on_finish=False,
        )
        elapsed = time.perf_counter() - start
        stats = consumer.pipeline.get_statistics()
        consumer.close()

        processed = stats["pipeline"]["total_processed"]
        results.append(
            {
                "batch_size": size,
                "processed": processed,
                "elapsed_seconds": round(elapsed, 4),
                "per_record_ms": round((elapsed / processed) * 1000.0, 3) if processed else None,
            }
        )

    return {
        "method": "One batch per run with isolated fresh SQL/Mongo/metadata state per size",
        "endpoint": "http://127.0.0.1:8000/record/{count}",
        "results": results,
    }


def main() -> None:
    batch_sizes = [10, 20, 50, 100, 500, 1000]
    payload = run_benchmark(batch_sizes)

    output_path = Path("docs") / "ingestion_benchmark_results.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
