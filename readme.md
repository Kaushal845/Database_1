# Synthetic Data Streaming API

A simple FastAPI-based synthetic data generator that produces realistic JSON records for use in database coursework, ingestion experiments, and cleaning/ETL tasks. Works fully offline and supports batch record streaming.

## ⚙️ Installation

Clone the repository:

```bash
git clone https://github.com/YogeshKMeena/Course_Resources.git
cd Course_Resources
```


## Features
- Generates randomized realistic JSON records
- Optional missing/null fields
- Single or batch streaming (`/record/{count}`)
- Suitable for SQL & NoSQL ingestion
- Works offline
- Optional Docker support

## Run Locally
```bash
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
````

Then open:

```
http://127.0.0.1:8000
```

## Endpoints

**Single record**

```
GET /
```

**Multiple records**

```
GET /record/{count}
```

## Example

```json
{
  "username": "sloantimothy",
  "name": "Veronica Williamson",
  "ip_address": "22.2.183.169",
  "device_model": "OnePlus 12",
  "app_version": "v1.5.5",
  "altitude": 542.26,
  "country": "Cook Islands",
  "postal_code": "74596",
  "session_id": "473af720-92e2-4c52-9825-db272121d36d",
  "steps": 11994,
  "spo2": 98,
  "sleep_hours": 4.1,
  "weather": "stormy",
  "temperature_c": 23.1,
  "item": null,
  "payment_status": "pending",
  "language": "Akan",
  "cpu_usage": 21,
  "is_active": true,
  "avatar_url": "https://picsum.photos/652/342",
  "metadata": {
    "sensor_data": {
      "version": "2.1",
      "calibrated": false,
      "readings": [
        10,
        8,
        10
      ]
    },
    "tags": [
      "out"
    ],
    "is_bot": false
  }
}
```


