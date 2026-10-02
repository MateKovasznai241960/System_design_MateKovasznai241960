---
title: AirBreda Architecture Design Document
permalink: /
---

# AirBreda: System design and cloud platforms elective

AirBreda project collects air quality and traffic data for one motorway interchange in Breda, stores it on AWS, and shows it on a small dashboard with a predicted NO2 value and the risk that NO2 is too high.

- **Air quality:** hourly NO2 from RIVM Luchtmeetnet, station **NL10240** (Breda-Tilburgseweg).
- **Traffic:** vehicle counts and speeds from NDW for four road sensors on the **A27 near hectometre 63**:

  | Site | What it measures |
  |---|---|
  | `hrl` | A27 mainline, direction 1 |
  | `hrr` | A27 mainline, direction 2 |
  | `vwd` | entry slip road |
  | `vwa` | exit slip road |

Everything in this folder runs as three Docker containers on one AWS server instance. The design and the reasons behind it are in the Architecture Design Document ([`docs/ADR.md`](docs/ADR.md)).

```
Luchtmeetnet ──> air-ingest ─────┐
                                 ├──> PostgreSQL (RDS) ──┐
NDW ───────────> traffic-ingest ─┤                       ├──> dashboard :8000 ──> browser
                                 └──> S3 bucket (CSV) ───┘     (model.pkl inside)
```

## What is in this folder

| File | What it does |
|---|---|
| `ingest_air.py` | Fetches the last ~50 hours of NO2 for NL10240 every hour and writes them to the database. |
| `getNO2Readings.py` | The Luchtmeetnet API call used by `ingest_air.py`. |
| `ingest_traffic.py` | Downloads the national NDW file every hour, keeps the four A27 sites, saves one CSV per site to S3 and writes the valid readings to the database. |
| `getTrafficReadings.py` | The NDW download and XML parsing used by `ingest_traffic.py`, including the four site IDs. |
| `dashboard.py` | FastAPI app: the web page, `/site/{id}` and `/health`. |
| `predict.py` | Loads `model.pkl` and turns a prediction into a risk score. |
| `model.pkl` | The trained linear regression model, built into the dashboard image. |
| `build_training_data.py` | Builds `training_data.csv` from the readings stored in the database. |
| `training_data.csv` | The exact rows the current `model.pkl` was trained on. |
| `train.py` | Trains the model on `training_data.csv` and writes `model.pkl`. |
| `Dockerfile`, `Dockerfile.traffic`, `Dockerfile.dashboard` | One image per service. |
| `docker-compose.yml` | Starts the three containers on one private network. |
| `requirements.txt` | Packages for the air image and the dashboard image, and for training. |
| `requirements-traffic.txt` | Packages for the traffic image. |
| `test_*.py`, `pytest.ini`, `requirements-dev.txt` | The 15 automated tests and what they need. |
| `.dockerignore` | Keeps secrets, tests and local data out of the Docker build. |
| `docs/ADR.md` | The Architecture Design Document: diagrams, decision records, costs and reflection. |

## Requirements

- Docker with the Compose plugin.
- A PostgreSQL database (we use Amazon RDS PostgreSQL 17) with the table below.
- An S3 bucket for the traffic CSVs.
- Python 3.11 or newer, only for running the tests or retraining the model outside Docker.

### Database table

Create it once:

```sql
CREATE TABLE sensor_readings (
    station_id  VARCHAR(20)   NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    component   VARCHAR(10)   NOT NULL,
    value       FLOAT,
    is_flagged  BOOLEAN       DEFAULT FALSE,
    PRIMARY KEY (station_id, timestamp, component)
);
```

| Data | `station_id` | `component` | `timestamp` |
|---|---|---|---|
| NO2 | `NL10240` | `NO2` | the **end** of the hourly average |
| Traffic | `NDW_<site>`, e.g. `NDW_hrl` | `flow_<lane>` or `speed_<lane>` | the start of the measured minute |

All times are in UTC.

### Settings (`.env`)

All settings come from environment variables. Compose reads them from a file called `.env` in this folder. That file holds a password, so it is **never committed**; create it yourself:

```dotenv
# Database (all three services)
PGHOST=<your RDS endpoint>
PGPORT=5432
PGDATABASE=<database name>
PGUSER=<user>
PGPASSWORD=<password>
PGSSLMODE=require

# Bucket (traffic-ingest and dashboard)
AIRBREDA_BUCKET=<bucket name>
AWS_REGION=eu-west-1
```

On the EC2 server, the containers get AWS permissions from the server's IAM role, so no AWS keys are needed. On a laptop, use an AWS profile or the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` variables. Never put keys in the code or the Dockerfiles.

## Running it

From inside this folder:

```bash
docker compose up -d --build     # build and start all three containers
docker compose ps                # check they are running
docker compose logs -f           # follow the logs
docker compose down              # stop everything
```

Both fetchers run once at start-up and then at the start of every hour.

| Address | What it returns |
|---|---|
| `http://<server>:8000/` | The dashboard page with the four sites. |
| `http://<server>:8000/site/hrl` | The latest real NO2, the site's latest traffic, the predicted NO2 and the risk. `hrl` can be `hrr`, `vwd` or `vwa`. |
| `http://<server>:8000/health` | The status of both fetchers: last successful fetch and bad-data count. |
| `http://localhost:8001/health`, `:8002/health` | Each fetcher's own health. These ports are open on the server itself only. |


| Situation | What `/site/{id}` does |
|---|---|
| The prediction fails | Still answers 200 with the real readings. The two prediction fields are `null`. |
| The database or bucket can't be reached | Answers 500. |
| No reading yet | Answers 503. |

Each script can also run once by hand, for example `python ingest_air.py --no-db` (fetch and log only) or `python ingest_traffic.py --no-upload` (no S3 upload).


## How the data is handled

**Duplicates are safe.** Every reading is identified by station, time and component, and a reading that is already stored is skipped. Running a fetcher twice changes nothing.

**Bad data.** Each fetcher checks its own source:
- **NO2:** a missing value, or one that stays the same for 3 or more hours in a row, is **kept and flagged** (`is_flagged = TRUE`).
- **Traffic:** a speed of `-1` is NDW's code for "no value". It is **not written to the database**, but the CSV in S3 keeps every value exactly as NDW sent it.
- Both log a `DATA_QUALITY_ERROR` warning. More than 10 warnings within an hour logs one `BAD_DATA_THRESHOLD_EXCEEDED` error, and the count is shown on `/health`.

**Logs.** Every event is one JSON line on stdout (for example `fetch_success`, `db_write`, `site_served`), with the level INFO, WARNING or ERROR. Docker keeps them on the server; read them with `docker compose logs`.

**Exit codes.** Every fetcher run ends with one of four codes:

| Code | Meaning |
|---|---|
| 0 | OK |
| 1 | The source could not be reached |
| 2 | The source sent unusable data |
| 3 | Our own storage failed |

## The model and how to reproduce it

`model.pkl` is a scikit-learn **linear regression** that predicts NO2 (µg/m³) from two inputs:
- the site's traffic: the sum of all its lanes' flows in one minute, in vehicles per hour;
- the hour of the day, in UTC.

The risk score passes the prediction through an S-curve around the EU limit of 40 µg/m³: 0.5 at 40, 0.12 at 30 and 0.88 at 50.

The model is trained on the pipeline's own stored data. `build_training_data.py` makes one row per site per hour: that site's traffic in the one minute stored for the hour, matched with the NO2 average of that same hour. This is exactly the input the dashboard sends at prediction time.

The current model was trained on 48 rows (12 hours × 4 sites, 1–2 October 2026) and learned:

```
NO2 ≈ 27.95 + 0.0031 × traffic + 0.24 × hour        (R² 0.34 on the training rows)
```

To retrain:

1. **Build the training data.** This needs the database settings, so run it where they are set. On the server, inside the air container:
   ```bash
   docker cp build_training_data.py air-ingest:/tmp/
   docker exec -w /tmp air-ingest python build_training_data.py /tmp/training_data.csv
   docker cp air-ingest:/tmp/training_data.csv .
   ```
   Or on a laptop that the database accepts, with the `PG*` variables set: `python build_training_data.py`.
2. **Train** (needs `pip install -r requirements.txt`):
   ```bash
   python train.py      # prints the row count, R², MAE and coefficients, writes model.pkl
   ```
   With the `training_data.csv` in this folder, it reproduces the current `model.pkl` exactly.
3. **Deploy** the new model: `docker compose up -d --build dashboard`.

The model is trained offline and built into the dashboard image. It does not change while the system runs, and the model that is served is always the one that was tested.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The 15 tests need no database, bucket or internet: the network, database and S3 calls are replaced by stubs. They cover:
- the air fetcher's retries and exit codes;
- the data-quality rules;
- the dashboard routes, including a failing prediction;
- loading the model.

The tests are kept out of the Docker images.
