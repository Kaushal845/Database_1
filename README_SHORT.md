# Assignment 2 Quick Run Guide

## 1) Install dependencies

```bash
pip install -r requirements.txt
```

## 2) Start generator API (terminal 1)

```bash
uvicorn main:app --reload --port 8000
```

## 3) Run ingestion (terminal 2)

```bash
python data_consumer.py
```

Optional:

```bash
python data_consumer.py 50 20
python data_consumer.py 50 20 path/to/schema.json
```

## 4) Run tests

```bash
pytest -q
```

## 5) Clean generated files

```bash
python quickstart.py clean
```

This cleans local artifacts and also drops project MongoDB databases:
- ingestion_db
- assignment2_test_db

Skip confirmation:

```bash
python quickstart.py clean --yes
```

Keep MongoDB data (files/cache only):

```bash
python quickstart.py clean --no-mongo
```

## 6) View docs

- docs/ASSIGNMENT2_TECHNICAL_REPORT.md
- docs/ARCHITECTURE_ASSIGNMENT2.md
- docs/CRUD_JSON_INTERFACE.md
