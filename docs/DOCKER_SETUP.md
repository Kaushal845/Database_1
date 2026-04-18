# Docker Setup Guide - Hybrid Database Framework


## What You Get

`docker compose up --build` starts:

1. `mongodb` container (MongoDB 7.0)
2. `app` container (FastAPI backend + built React dashboard)

Endpoints:

1. Dashboard: http://localhost:8000
2. API docs: http://localhost:8000/docs
3. Health: http://localhost:8000/health

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | 20.10+ |
| Docker Compose | v2+ |

Install Docker Desktop on Windows/macOS, or Docker Engine + Compose plugin on Linux.

## Quick Start

```bash
git clone https://github.com/Kaushal845/Database_1.git
cd Database_1
docker compose up --build -d
docker compose ps
```

Verify:

```bash
curl http://localhost:8000/health
```

Expected key fields:

1. `"status":"healthy"`
2. `"sql_connection":"ok"`
3. `"mongo_connection":"ok"`

## Service Map

| Service | Container | Host Port | Purpose |
|---------|-----------|-----------|---------|
| `mongodb` | `hybrid-db-mongo` | 27017 | Mongo backend |
| `app` | `hybrid-db-app` | 8000 | Dashboard + API |

## Core Commands

Start:

```bash
docker compose up --build -d
```

Stop:

```bash
docker compose down
```

Stop and remove volumes:

```bash
docker compose down -v
```

Logs:

```bash
docker compose logs -f app
docker compose logs -f mongodb
```

## Ingestion and Queries Inside Docker

Small ingestion test (non-interactive):

```bash
docker compose exec app sh -lc "printf '\n' | python data_consumer.py 5 1"
```

Larger ingestion:

```bash
docker compose exec app python data_consumer.py 100 5
```

Run a logical query through API:

```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"operation":"read","limit":3}'
```

Get dashboard summary:

```bash
curl http://localhost:8000/api/dashboard/summary
```

## Benchmarks in Docker

```bash
docker compose exec app python performance_benchmark.py
docker compose exec app python comparative_benchmark.py
curl http://localhost:8000/api/dashboard/benchmark-results
```

## Data Persistence Notes

Current compose volumes:

1. `mongo_data` persists MongoDB data at `/data/db`.
2. `./docs:/app/docs` keeps generated reports/benchmark artifacts on your host.
3. `app_data:/app/data` is mounted, but default SQLite/metadata files are currently written under `/app` by default.

Implication: Mongo persistence is guaranteed. SQL/metadata persistence depends on runtime path configuration.


### Port conflict

If 8000 or 27017 is in use, edit `ports` in [docker-compose.yml](docker-compose.yml).

## TA Demonstration Script (Suggested)

Use this exact flow:

1. Start clean:

```bash
docker compose down -v
docker compose up --build -d
docker compose ps
```

2. Show health:

```bash
curl http://localhost:8000/health
```

3. Show UI and API docs:

1. http://localhost:8000
2. http://localhost:8000/docs

4. Run ingestion:

```bash
docker compose exec app sh -lc "printf '\n' | python data_consumer.py 10 1"
```

5. Show records increased:

```bash
curl http://localhost:8000/api/dashboard/summary
```

6. Optional benchmark demo:

```bash
docker compose exec app python performance_benchmark.py
docker compose exec app python comparative_benchmark.py
curl http://localhost:8000/api/dashboard/benchmark-results
```

## File References

1. [Dockerfile](Dockerfile): multi-stage image build (dashboard build + Python runtime)
2. [docker-compose.yml](docker-compose.yml): service orchestration, ports, health checks, volumes
3. [.dockerignore](.dockerignore): trims Docker build context
