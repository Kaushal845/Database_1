"""
Comparative Benchmark Suite — Assignment 4

Compares performance of the hybrid framework's logical abstraction layer
versus direct SQL and MongoDB access for:
1. Record retrieval (framework read vs direct SQL SELECT vs direct MongoDB find)
2. Nested document access (framework vs direct MongoDB)
3. Record updates (framework vs direct SQL UPDATE vs direct MongoDB $set)
4. Record insertion (framework ingest vs direct SQL INSERT vs direct MongoDB insert_one)

Outputs:
  docs/comparative_benchmark_results.json
  docs/comparative_report.md
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List

from faker import Faker

from ingestion_pipeline import IngestionPipeline
from logging_utils import setup_logging

fake = Faker()
Faker.seed(42)

DOCS_DIR = Path("docs")
DOCS_DIR.mkdir(exist_ok=True)

ITERATIONS = 50  # Default iterations per test


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_pipeline(label: str) -> IngestionPipeline:
    meta = DOCS_DIR / f"comp_bench_meta_{label}.json"
    db = DOCS_DIR / f"comp_bench_{label}.db"
    for p in (meta, db):
        if p.exists():
            p.unlink()
    return IngestionPipeline(
        metadata_file=str(meta),
        sql_db=str(db),
        mongo_db=f"comp_bench_{label}",
        use_transactions=True,
    )


def _generate_record(idx: int) -> Dict[str, Any]:
    return {
        "username": f"comp_user_{idx}",
        "email": fake.email(),
        "age": fake.random_int(18, 80),
        "country": fake.country(),
        "score": round(fake.pyfloat(min_value=0, max_value=100), 2),
        "profile": {"bio": fake.sentence(), "website": fake.url()},
        "orders": [
            {"order_id": f"ord-{idx}-{j}", "item": fake.word(), "price": round(fake.pyfloat(min_value=1, max_value=500), 2)}
            for j in range(2)
        ],
    }


def _timed(func, iterations: int = ITERATIONS) -> Dict[str, float]:
    timings = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        timings.append((time.perf_counter() - start) * 1000.0)
    timings.sort()
    p95_idx = max(0, int(len(timings) * 0.95) - 1)
    return {
        "iterations": iterations,
        "avg_ms": round(statistics.mean(timings), 3),
        "min_ms": round(min(timings), 3),
        "max_ms": round(max(timings), 3),
        "p95_ms": round(timings[p95_idx], 3),
    }


# ---------------------------------------------------------------------------
# comparative tests
# ---------------------------------------------------------------------------

def compare_reads(pipeline: IngestionPipeline) -> Dict[str, Any]:
    """Compare read latency: framework vs direct SQL vs direct MongoDB."""
    qe = pipeline.query_engine
    sql = pipeline.sql_manager
    mongo = pipeline.mongo_manager

    # Framework read
    fw = _timed(lambda: qe.execute({"operation": "read", "filters": {"username": "comp_user_0"}, "limit": 10}))

    # Direct SQL
    def _direct_sql():
        sql.cursor.execute("SELECT * FROM ingested_records WHERE username = ? LIMIT 10", ("comp_user_0",))
        sql.cursor.fetchall()

    sq = _timed(_direct_sql)

    # Direct MongoDB
    def _direct_mongo():
        list(mongo._get_collection("ingested_records").find({"username": "comp_user_0"}).limit(10))

    mg = _timed(_direct_mongo)

    # Calculate overhead against combined baseline (framework queries BOTH backends)
    combined_baseline = sq["avg_ms"] + mg["avg_ms"]
    overhead_pct = round(((fw["avg_ms"] - combined_baseline) / combined_baseline) * 100, 1) if combined_baseline > 0 else 0

    return {
        "test_name": "Record Retrieval (single filter read)",
        "results": [
            {"method": "Framework (logical query)", **fw},
            {"method": "Direct SQL (SELECT)", **sq},
            {"method": "Direct MongoDB (find)", **mg},
        ],
        "overhead_pct": overhead_pct,
    }


def compare_nested_access(pipeline: IngestionPipeline) -> Dict[str, Any]:
    """Compare nested document access: framework vs direct MongoDB."""
    qe = pipeline.query_engine
    mongo = pipeline.mongo_manager

    # Framework — read user with nested entities
    fw = _timed(
        lambda: qe.execute({"operation": "read", "filters": {"username": "comp_user_5"}, "fields": ["username", "orders", "profile"]}),
        iterations=40,
    )

    # Direct MongoDB — fetch nested doc
    def _direct_mongo():
        list(mongo._get_collection("ingested_records").find(
            {"username": "comp_user_5"},
            {"username": 1, "orders": 1, "profile": 1}
        ))

    mg = _timed(_direct_mongo, iterations=40)

    overhead_pct = round(((fw["avg_ms"] - mg["avg_ms"]) / mg["avg_ms"]) * 100, 1) if mg["avg_ms"] > 0 else 0

    return {
        "test_name": "Nested Document Access (orders + profile)",
        "results": [
            {"method": "Framework (logical query)", **fw},
            {"method": "Direct MongoDB (find + projection)", **mg},
        ],
        "overhead_pct": overhead_pct,
    }


def compare_updates(pipeline: IngestionPipeline) -> Dict[str, Any]:
    """Compare update latency: framework vs direct SQL vs direct MongoDB."""
    qe = pipeline.query_engine
    sql = pipeline.sql_manager
    mongo = pipeline.mongo_manager

    # Find a record's sys_ingested_at for precise WHERE clause
    row = sql.cursor.execute("SELECT sys_ingested_at, username FROM ingested_records LIMIT 1").fetchone()
    sql_key = row[0] if row else None

    counter = [0]

    # Framework update (updates a MongoDB-stored field through the abstraction)
    def _fw_update():
        counter[0] += 1
        qe.execute({"operation": "update", "filters": {"username": "comp_user_1"}, "data": {"country": f"Country_{counter[0]}"}})

    fw = _timed(_fw_update, iterations=30)

    # Direct SQL — update the username in-place (guaranteed TEXT column)
    def _direct_sql():
        counter[0] += 1
        if sql_key:
            sql.cursor.execute("UPDATE ingested_records SET username = ? WHERE sys_ingested_at = ?", ("comp_user_1", sql_key))
            sql.connection.commit()

    sq = _timed(_direct_sql, iterations=30)

    # Direct MongoDB
    def _direct_mongo():
        counter[0] += 1
        mongo._get_collection("ingested_records").update_many(
            {"username": "comp_user_1"},
            {"$set": {"country": f"Country_{counter[0]}"}}
        )

    mg = _timed(_direct_mongo, iterations=30)

    # Calculate overhead against combined baseline (framework updates BOTH backends)
    combined_baseline = sq["avg_ms"] + mg["avg_ms"]
    overhead_pct = round(((fw["avg_ms"] - combined_baseline) / combined_baseline) * 100, 1) if combined_baseline > 0 else 0

    return {
        "test_name": "Record Update (single field)",
        "results": [
            {"method": "Framework (logical update)", **fw},
            {"method": "Direct SQL (UPDATE)", **sq},
            {"method": "Direct MongoDB ($set)", **mg},
        ],
        "overhead_pct": overhead_pct,
    }


def compare_inserts(pipeline: IngestionPipeline) -> Dict[str, Any]:
    """Compare insert latency: framework vs direct backends."""
    qe = pipeline.query_engine
    sql = pipeline.sql_manager
    mongo = pipeline.mongo_manager

    counter = [1000]

    # Framework insert
    def _fw_insert():
        counter[0] += 1
        rec = _generate_record(counter[0])
        pipeline.ingest_record(rec)

    fw = _timed(_fw_insert, iterations=20)

    # Direct SQL insert — only use columns that actually exist
    sql_columns = [row[1] for row in sql.connection.execute("PRAGMA table_info(ingested_records)").fetchall()]
    def _direct_sql():
        counter[0] += 1
        import datetime as _dt
        ts = _dt.datetime.now(_dt.timezone.utc).isoformat() + f"_sql_{counter[0]}"
        # Build INSERT using only existing columns
        cols = ["sys_ingested_at"]
        vals = [ts]
        if "username" in sql_columns:
            cols.append("username")
            vals.append(f"direct_sql_{counter[0]}")
        sql.cursor.execute(
            f"INSERT INTO ingested_records ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
            vals,
        )
        sql.connection.commit()

    sq = _timed(_direct_sql, iterations=20)

    # Direct MongoDB insert
    def _direct_mongo():
        counter[0] += 1
        import datetime as _dt
        ts = _dt.datetime.now(_dt.timezone.utc).isoformat() + f"_mongo_{counter[0]}"
        mongo._get_collection("ingested_records").insert_one({
            "sys_ingested_at": ts,
            "username": f"direct_mongo_{counter[0]}",
            "email": f"mongo_{counter[0]}@test.com",
            "age": 25,
            "score": 50.0,
        })

    mg = _timed(_direct_mongo, iterations=20)

    # Calculate overhead against combined baseline (framework inserts into BOTH backends)
    combined_baseline = sq["avg_ms"] + mg["avg_ms"]
    overhead_pct = round(((fw["avg_ms"] - combined_baseline) / combined_baseline) * 100, 1) if combined_baseline > 0 else 0

    return {
        "test_name": "Record Insertion",
        "results": [
            {"method": "Framework (full pipeline)", **fw},
            {"method": "Direct SQL (INSERT)", **sq},
            {"method": "Direct MongoDB (insert_one)", **mg},
        ],
        "overhead_pct": overhead_pct,
    }


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_logging("WARNING")
    print("=" * 60)
    print("Comparative Benchmark: Framework vs Direct Access")
    print("=" * 60)

    # Clean up any stale MongoDB databases from previous runs
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        client.drop_database("comp_bench_compare")
        client.close()
        print("  Cleaned up previous MongoDB benchmark data")
    except Exception:
        pass

    # Seed data
    print("\n  Seeding 100 records...")
    pipeline = _make_pipeline("compare")
    for i in range(100):
        pipeline.ingest_record(_generate_record(i))
    print("  Done.\n")

    comparisons = []

    # 1. Reads
    print("[1/4] Comparing read performance...")
    try:
        result = compare_reads(pipeline)
        comparisons.append(result)
        for r in result["results"]:
            print(f"  {r['method']}: avg={r['avg_ms']:.2f}ms")
        print(f"  -> Framework overhead: {result['overhead_pct']}%\n")
    except Exception as e:
        print(f"  [X] Read benchmark failed: {e}\n")

    # 2. Nested access
    print("[2/4] Comparing nested document access...")
    try:
        result = compare_nested_access(pipeline)
        comparisons.append(result)
        for r in result["results"]:
            print(f"  {r['method']}: avg={r['avg_ms']:.2f}ms")
        print(f"  -> Framework overhead: {result['overhead_pct']}%\n")
    except Exception as e:
        print(f"  [X] Nested access benchmark failed: {e}\n")

    # 3. Updates
    print("[3/4] Comparing update performance...")
    try:
        result = compare_updates(pipeline)
        comparisons.append(result)
        for r in result["results"]:
            print(f"  {r['method']}: avg={r['avg_ms']:.2f}ms")
        print(f"  -> Framework overhead: {result['overhead_pct']}%\n")
    except Exception as e:
        print(f"  [X] Update benchmark failed: {e}\n")

    # 4. Inserts
    print("[4/4] Comparing insert performance...")
    try:
        result = compare_inserts(pipeline)
        comparisons.append(result)
        for r in result["results"]:
            print(f"  {r['method']}: avg={r['avg_ms']:.2f}ms")
        print(f"  -> Framework overhead: {result['overhead_pct']}%\n")
    except Exception as e:
        print(f"  [X] Insert benchmark failed: {e}\n")

    pipeline.close()

    # Build summary table
    summary = []
    for comp in comparisons:
        row = {"metric": comp["test_name"], "overhead_pct": comp["overhead_pct"]}
        for r in comp["results"]:
            if "Framework" in r["method"]:
                row["framework_ms"] = r["avg_ms"]
            elif "SQL" in r["method"]:
                row["direct_sql_ms"] = r["avg_ms"]
            elif "MongoDB" in r["method"] or "Mongo" in r["method"]:
                row["direct_mongo_ms"] = r["avg_ms"]
        summary.append(row)

    # Save results
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "comparisons": comparisons,
        "summary": summary,
    }

    out_path = DOCS_DIR / "comparative_benchmark_results.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[OK] Results saved to {out_path}")

    _generate_report(payload)
    print(f"[OK] Report saved to {DOCS_DIR / 'comparative_report.md'}")


def _generate_report(data: Dict[str, Any]) -> None:
    lines = [
        "# Comparative Analysis Report",
        "",
        f"Generated: {data['generated_at']}",
        "",
        "## Summary",
        "",
        "| Test | Framework (ms) | Direct SQL (ms) | Direct MongoDB (ms) | Overhead |",
        "|------|---------------|----------------|--------------------:|----------|",
    ]
    for row in data["summary"]:
        fw = f"{row['framework_ms']:.2f}" if isinstance(row.get('framework_ms'), (int, float)) else "N/A"
        sq = f"{row['direct_sql_ms']:.2f}" if isinstance(row.get('direct_sql_ms'), (int, float)) else "N/A"
        mg = f"{row['direct_mongo_ms']:.2f}" if isinstance(row.get('direct_mongo_ms'), (int, float)) else "N/A"
        lines.append(
            f"| {row['metric']} | {fw} | {sq} | {mg} | {row['overhead_pct']}% |"
        )

    for comp in data["comparisons"]:
        lines += [
            "",
            f"## {comp['test_name']}",
            "",
            f"Framework overhead: **{comp['overhead_pct']}%**",
            "",
            "| Method | Avg (ms) | Min (ms) | Max (ms) | P95 (ms) |",
            "|--------|---------|---------|---------|---------|",
        ]
        for r in comp["results"]:
            lines.append(
                f"| {r['method']} | {r['avg_ms']:.3f} | {r['min_ms']:.3f} | {r['max_ms']:.3f} | {r['p95_ms']:.3f} |"
            )

    lines += [
        "",
        "## Discussion",
        "",
        "### Where the abstraction adds overhead",
        "",
        "- **Read operations**: The framework performs metadata lookups, routes queries to both SQL and MongoDB, then merges results. This adds measurable latency compared to a single direct query.",
        "- **Ingestion**: The framework runs type detection, placement heuristics, normalization, and dual-backend writes — significantly slower than a single raw INSERT or insert_one.",
        "- **Updates**: The framework routes fields to the correct backend based on metadata, updating SQL and MongoDB separately. Direct updates on a single backend are naturally faster.",
        "",
        "### Where the abstraction provides value",
        "",
        "- **Unified access**: Users query a single logical interface without knowing which backend stores each field. This eliminates the need to write separate SQL and MongoDB queries.",
        "- **Automatic schema evolution**: New fields are automatically detected, classified, and routed to the optimal backend without manual schema changes.",
        "- **Nested entity management**: The framework handles normalization of arrays into child tables, embedded MongoDB documents, and reference-based entities — all transparently.",
        "- **ACID guarantees**: The 2-phase commit ensures atomic operations across both backends, which would require custom implementation with direct access.",
        "- **Backend-agnostic applications**: Application code doesn't need to change if data moves between backends (e.g. during schema evolution or optimization).",
        "",
        "### When to use the framework vs direct access",
        "",
        "| Scenario | Recommended Approach |",
        "|----------|---------------------|",
        "| Schema-flexible, evolving data | Framework |",
        "| High-throughput, fixed-schema writes | Direct backend |",
        "| Cross-backend queries (SQL + MongoDB) | Framework |",
        "| Single-backend, latency-critical reads | Direct backend |",
        "| Need ACID across SQL + MongoDB | Framework |",
        "",
    ]

    (DOCS_DIR / "comparative_report.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
