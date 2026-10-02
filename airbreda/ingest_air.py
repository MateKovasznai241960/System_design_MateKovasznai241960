"""
Ingest NO2 readings for Luchtmeetnet station NL10240 (Breda-Tilburgseweg) into
the PostgreSQL table sensor_readings (sql/001_create_sensor_readings.sql).

Builds on get_no2_measurements() from getNO2Readings.py. The data is fetched live
from the Luchtmeetnet open API every time this runs: one request per run, at most
MAX_ATTEMPTS (3) if the API is briefly unreachable. That is far below the fair-use
limit of 100 requests / 5 minutes. Every NO2 row of the fetched page (about 50
hours) is written with INSERT ... ON CONFLICT DO NOTHING, in one transaction, so
running the script twice adds no duplicate rows (task 005: only the is_flagged
flag of an existing row can still change, from FALSE to TRUE, D-039).

Output: one JSON object per line on stdout (task 005, D-038), e.g.
    {"event": "fetch_success", "source": "Luchtmeetnet", "station_id": "NL10240", ...}

Usage:
    python ingest_air.py            # fetch, log, write to the database
    python ingest_air.py --no-db    # fetch and log only (no database needed)
    python ingest_air.py --serve    # long-running: fetch every hour, GET /health on port 8001 (D-041)

Database settings come from environment variables, never from code:
    PGHOST, PGDATABASE, PGUSER, PGPASSWORD (required), PGPORT (default 5432),
    PGSSLMODE (default "require": the connection must be encrypted).

Exit codes:
    0  success (also when the newest value is null: it is logged as null, see below)
    1  upstream unreachable: timeout, connection error or HTTP error status,
       after the retries below or their time budget were used up (or straight
       away for a 4xx)
    2  bad response: API answered but the data was unusable (not JSON, wrong
       shape, empty, no NO2 rows, missing field, non-numeric value,
       unparseable timestamp). Never retried.
    3  could not store the result: database settings missing, database
       unreachable, login refused, table missing or the insert failed. The
       transaction is rolled back, so nothing is half-written (D-020).
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values

from getNO2Readings import FORMULA, STATION, get_no2_measurements


# --- Structured logging (task 005, D-038) --------------------------------------
# Every event is one JSON object (json.dumps of a dict) on one line of stdout.
# Our own named logger, not the root logger: other libraries log to the root,
# and their plain-text lines must not end up between our JSON lines. The stdout
# handler is attached in the command-line entry point at the bottom, so tests
# see the events through pytest's caplog.
SOURCE = "Luchtmeetnet"
log = logging.getLogger("ingest_air")
log.setLevel(logging.INFO)

# --- Data quality counter and /health state (task 005, D-040 to D-042) ---------
luchtmeetnet_bad_data_count = 0  # +1 for every DATA_QUALITY_ERROR WARNING logged
last_successful_fetch = None     # UTC time of the last successful fetch, "...Z"
_reported_flagged = set()        # timestamps already reported: a re-fetch must not count them again
THRESHOLD = 10                   # more than 10 warnings in a rolling hour -> one ERROR event
WINDOW_S = 3600.0
_warning_times = deque()         # time.monotonic() of each warning still in the window
_threshold_logged = False

# --- Retry policy for brief outages (docs/decisions.md D-010, D-018) -----------
# Only errors a retry can fix are retried: timeouts, connection/DNS errors,
# HTTP 5xx and 429. A 4xx means our request is wrong and bad data (exit 2) means
# the API changed; retrying those only adds load. Backoff is 1 s, then 2 s.
# SSLError and ProxyError are retried too: requests makes them subclasses of
# ConnectionError, and an SSLError can be a permanent certificate problem or a
# transient broken handshake. Retrying a permanent one costs 2 extra requests
# and 3 s, and the exit code (1) is the same either way, so we keep it simple.
#
# Run time: the request COUNT is hard-bounded (3, far below the fair-use limit
# of 100 requests / 5 minutes). The TIME is only partly bounded. requests'
# timeout=10 is per connect attempt and per socket read, not per request: DNS
# lookup is not covered, the API host has many addresses (tried one by one, each
# with its own 10 s), and a slow body can take longer than 10 s in total. So one
# attempt can take minutes in a bad network. TOTAL_BUDGET_S limits the retries:
# we never sleep, and never start another attempt, past the budget. It cannot
# interrupt an attempt that is already running, so the real hard stop must be
# the scheduler's timeout (e.g. an ECS/Lambda timeout) in a later task.
MAX_ATTEMPTS = 3
BACKOFF_BASE_S = 1.0
TOTAL_BUDGET_S = 30.0


def is_retryable(exc: Exception) -> bool:
    """True for failures that a later attempt could plausibly fix."""
    # Timeout, ConnectionError (includes DNS, SSLError, ProxyError: see above).
    if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        return status == 429 or 500 <= status <= 599
    return False  # e.g. JSONDecodeError, other 4xx, invalid URL


def fetch_with_retry(fetch, sleep=time.sleep, clock=time.monotonic):
    """Call fetch(); retry retryable errors with exponential backoff.

    `sleep` and `clock` are injectable so tests can run without really waiting.
    Non-retryable errors, the last error after MAX_ATTEMPTS, and the last error
    when TOTAL_BUDGET_S is used up are re-raised unchanged, so main() maps them
    to exit codes exactly as before (D-003).
    """
    deadline = clock() + TOTAL_BUDGET_S
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fetch()
        except requests.exceptions.RequestException as exc:
            if attempt == MAX_ATTEMPTS or not is_retryable(exc):
                raise
            delay = BACKOFF_BASE_S * 2 ** (attempt - 1)
            # One check covers both "before sleeping" and "before the next
            # attempt": if the sleep would end at or past the deadline, the
            # next attempt would start too late, so give up now.
            if clock() + delay >= deadline:
                log.warning(json.dumps({
                    "event": "fetch_give_up", "source": SOURCE,
                    "attempt": attempt, "max_attempts": MAX_ATTEMPTS,
                    "error": type(exc).__name__, "retry_budget_s": TOTAL_BUDGET_S,
                }))
                raise
            log.warning(json.dumps({
                "event": "fetch_retry", "source": SOURCE,
                "attempt": attempt, "max_attempts": MAX_ATTEMPTS,
                "error": type(exc).__name__, "retry_in_s": delay,
            }))
            sleep(delay)


# --- Which trade-off the sensor network makes (CAP / PACELC), D-017 ------------
# Strictly, CAP is about replicated data during a network partition: a system
# must then either answer with what it has (A) or refuse until it is sure (C).
# Here the "nodes" are the measuring stations and RIVM's central database/API.
# - Partition (PA): if a station's link is down, the central API keeps
#   answering with what it has (the newest hours are simply missing) instead of
#   returning errors. Choosing C on the READ side would not lose data, it would
#   make the API return errors or be unavailable. On the WRITE side (recording
#   a measurement) refusing is worse: the air at 10:00 can never be measured
#   again, so an unrecorded hour is lost for good. (Whether stations buffer and
#   upload late is not documented by the open API, so we do not rely on it.)
# - Else, no partition (EL, the "else" half of PACELC): RIVM publishes new
#   values straight away, unvalidated, instead of waiting for validation. That
#   is Latency over Consistency.
# - Validation is NOT eventual consistency. Eventual consistency means replicas
#   converge on the same value once writes stop. A validation correction is a
#   NEW write by a second writer (RIVM) in a data-quality lifecycle
#   (provisional -> validated): the value may change or become null.
# Why this suits sensor networks: a provisional value now is more useful for
# public air-quality information than none, and conflicts are rare because each
# station is the only source of its measurements and later changes come only
# from RIVM validation. Consequence for us: the same hour can differ between two
# fetches. What the database does with that is explained at store_readings()
# below (first-seen value wins, D-019).
#
# --- Missing data: null values AND missing rows (D-012, D-016) -----------------
# This API shows "no measurement for this hour" in two ways:
# 1. A row with value null (outage, maintenance, invalidated value). At ingest
#    we KEEP the row and FLAG it (value_missing = True).
# 2. No row at all: the hour is just left out (seen live: 2026-09-21 07:00Z).
#    Counting nulls cannot find this. Gaps are found only by comparing the
#    series against the expected hourly index (every hour between the oldest
#    and newest timestamp). Here we only COUNT them (missing_hours()) so the
#    output does not give false comfort. Absent hours are NOT inserted into
#    sensor_readings (D-019): under ON CONFLICT DO NOTHING a placeholder null
#    would block the real value if it arrived late. A downstream transform
#    step (e.g. training), not the sensor_readings insert, can reindex on the
#    hour and flag them, so both kinds of gap look the same there.
# We never silently drop (hides the gap, "no measurement" would look like "not
# ingested") and never impute (an invented number looks real and cannot be
# undone). Filling gaps is left to an explicit downstream step (e.g. training)
# that documents its method and marks imputed values. A missing FIELD is
# different: it means the API schema changed, so exit 2.
REQUIRED_FIELDS = ("formula", "value", "timestamp_measured")


def parse_utc_timestamp(raw) -> datetime:
    """Parse the API's ISO 8601 timestamp and normalise it to UTC.

    Raises ValueError if it is unusable.
    """
    if not isinstance(raw, str):
        raise ValueError(f"timestamp is not a string: {raw!r}")
    ts = datetime.fromisoformat(raw)  # raises ValueError on bad format
    if ts.tzinfo is None:
        # Without an offset we cannot know which time it is - refuse to guess.
        raise ValueError(f"timestamp has no UTC offset: {raw!r}")
    return ts.astimezone(timezone.utc)


def records_to_frame(records: list) -> pd.DataFrame:
    """Turn API records (formula, value, timestamp_measured) into a DataFrame
    with columns component, value, timestamp (UTC) and value_missing.

    Raises ValueError for bad data (missing field, non-numeric value, bad timestamp).
    Null values are kept and flagged, not dropped (see the policy above).
    """
    rows = []
    for record in records:
        for field in REQUIRED_FIELDS:
            if field not in record:
                raise ValueError(f"measurement is missing field {field!r}")
        value = record["value"]
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
            raise ValueError(f"measurement value is not a number: {value!r}")
        try:
            ts = parse_utc_timestamp(record["timestamp_measured"])
        except ValueError as exc:
            raise ValueError(f"unparseable timestamp_measured: {exc}") from exc
        rows.append({"component": record["formula"], "value": value, "timestamp": ts})

    df = pd.DataFrame(rows, columns=["component", "value", "timestamp"])
    df["value_missing"] = df["value"].isna()
    return df


def filter_no2_readings(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the NO2 rows. Rows whose value is null are kept."""
    return df[df["component"] == FORMULA].reset_index(drop=True)


def stale_or_null(df: pd.DataFrame) -> pd.Series:
    """True for rows whose value is null, or that belong to a run of 3 or more
    consecutive hourly timestamps with the same value (task 005, D-040).

    ALL rows of such a run are flagged (student decision), not only the 3rd
    one onwards. A gap in the hours breaks a run, and so does a null (NaN ==
    NaN is False in pandas). Returns a boolean Series aligned with df's index.
    """
    ordered = df.sort_values("timestamp")
    same_as_previous = (
        (ordered["value"] == ordered["value"].shift())
        & (ordered["timestamp"].diff() == pd.Timedelta(hours=1))
    )
    run_id = (~same_as_previous).cumsum()  # a new run starts where the value or the 1-hour step breaks
    run_length = ordered.groupby(run_id)["value"].transform("size")
    flagged = ordered["value"].isna() | (run_length >= 3)
    return flagged.reindex(df.index)


def record_bad_data(clock=time.monotonic) -> None:
    """Count one DATA_QUALITY_ERROR and apply the rolling-hour threshold (D-042).

    Logs BAD_DATA_THRESHOLD_EXCEEDED once when the warnings of the last hour
    pass THRESHOLD, and not again until that count has dropped back to
    THRESHOLD or below.
    """
    global luchtmeetnet_bad_data_count, _threshold_logged
    luchtmeetnet_bad_data_count += 1
    now = clock()
    while _warning_times and _warning_times[0] <= now - WINDOW_S:
        _warning_times.popleft()
    if len(_warning_times) <= THRESHOLD:
        _threshold_logged = False  # at or below the threshold again: may alert again
    _warning_times.append(now)
    if len(_warning_times) > THRESHOLD and not _threshold_logged:
        log.error(json.dumps({"event": "BAD_DATA_THRESHOLD_EXCEEDED", "source": SOURCE,
                              "count": len(_warning_times)}))
        _threshold_logged = True


def missing_hours(timestamps: pd.Series) -> int:
    """Count the hours absent from the fetched window (oldest to newest, hourly).

    Hours after the newest timestamp are not counted: that is publication lag,
    not a gap. Uses a set difference instead of reindex(), so a duplicated
    timestamp cannot crash it.
    """
    expected = pd.date_range(timestamps.min(), timestamps.max(), freq="h")
    return len(expected.difference(pd.DatetimeIndex(timestamps)))


# --- Storage: sensor_readings with ON CONFLICT DO NOTHING (D-019, D-020) -------
# Idempotency: the primary key (station_id, timestamp, component) identifies one
# hourly reading. Each run sends the whole fetched page (~50 hours); the hours
# already in the table are skipped, only new hours are inserted. Running twice
# therefore gives the same row count.
#
# TRADE-OFF (the task mandates DO NOTHING; D-017 asked for an upsert):
# RIVM revises values after publication (provisional -> validated, D-017).
# DO NOTHING keeps the FIRST value we ever saw for an hour. A later correction
# is silently ignored, and a null that RIVM later fills in stays null. An
# upsert (ON CONFLICT ... DO UPDATE SET value = EXCLUDED.value) would pick up
# corrections that arrive while the hour is still on the fetched page (~50 h),
# but then the table no longer shows what we saw at the time.
# So the table means "first-seen provisional values", not "validated values".
# What makes a later fix possible is keeping raw data outside the table:
# RIVM itself keeps the history (re-fetchable within fair use), and a raw air
# copy in the S3 bucket (an open question in task 003; for traffic the bucket
# holds the parsed per-site CSVs, see ingest_traffic.py) could be reprocessed
# into a corrected table.
# For the same reason absent hours (D-016) are NOT inserted as null rows: with
# DO NOTHING a placeholder null would block the real value if it arrived late.
# Gaps are found with a query against the expected hours instead.
DB_REQUIRED_ENV = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
DB_CONNECT_TIMEOUT_S = 10

# Task 005 (D-039, partly supersedes D-019): the value is still first-seen and
# never overwritten. Only is_flagged may change, and only from FALSE to TRUE:
# each run re-fetches ~50 hours, so hours 1 and 2 of a stale run are usually
# inserted (unflagged) before hour 3 arrives and makes it a run. RETURNING 1
# now counts the rows that were inserted or newly flagged.
INSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value, is_flagged)
    VALUES %s
    ON CONFLICT (station_id, timestamp, component) DO UPDATE SET is_flagged = TRUE
        WHERE EXCLUDED.is_flagged AND sensor_readings.is_flagged IS NOT TRUE
    RETURNING 1
"""


def db_settings_from_env(env=os.environ) -> dict:
    """Read connection settings from PG* environment variables.

    Raises ValueError naming the missing variables (never their values).
    """
    missing = [name for name in DB_REQUIRED_ENV if not env.get(name)]
    if missing:
        raise ValueError(f"database settings missing: set {', '.join(missing)}")
    return {
        "host": env["PGHOST"],
        "port": int(env.get("PGPORT", "5432")),
        "dbname": env["PGDATABASE"],
        "user": env["PGUSER"],
        "password": env["PGPASSWORD"],
        # "require" = the connection must be encrypted. RDS also enforces TLS
        # on the server side (rds.force_ssl = 1 in infra/airbreda.yaml). Only a
        # local test container without TLS should set PGSSLMODE=disable.
        "sslmode": env.get("PGSSLMODE", "require"),
        "connect_timeout": DB_CONNECT_TIMEOUT_S,
    }


def frame_to_rows(df: pd.DataFrame) -> list:
    """(station_id, timestamp UTC, component, value, is_flagged) tuples; NaN becomes NULL."""
    return [
        (
            STATION,
            row.timestamp.to_pydatetime(),  # tz-aware UTC -> TIMESTAMPTZ
            row.component,
            None if pd.isna(row.value) else float(row.value),  # null stays NULL (D-012)
            bool(row.is_flagged),  # stale or null: still written, but flagged (task 005)
        )
        for row in df.itertuples(index=False)
    ]


def store_readings(df: pd.DataFrame, settings: dict) -> int:
    """Insert the rows in one transaction; return how many were new.

    Raises psycopg2.Error if the database is unreachable or the insert fails;
    the transaction is then rolled back, so a run never half-writes.
    """
    rows = frame_to_rows(df)
    conn = psycopg2.connect(**settings)
    try:
        with conn:  # commit on success, roll back on any exception
            with conn.cursor() as cur:
                # One batched statement instead of one round trip per row.
                # RETURNING 1 + fetch=True counts only the rows really inserted.
                inserted = execute_values(cur, INSERT_SQL, rows, fetch=True)
    finally:
        conn.close()
    return len(inserted)


def utc_z(ts: datetime) -> str:
    """Aware datetime -> UTC ISO 8601 with Z, e.g. 2024-01-15T09:00:00Z."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_error(event: str, error: str) -> None:
    """One ERROR event. `event` names the exit-code meaning (D-003/D-020)."""
    log.error(json.dumps({"event": event, "source": SOURCE, "error": error}))


def main(sleep=time.sleep, store=None) -> int:
    """Fetch, validate and log; then call store(no2_frame) if given.

    `store` is injected by the command-line entry point below. Tests call
    main() without it, so they never need a database.
    """
    global last_successful_fetch
    try:
        records = fetch_with_retry(lambda: get_no2_measurements(STATION, FORMULA), sleep=sleep)
    # Must come first: JSONDecodeError subclasses BOTH RequestException and
    # ValueError, so it would otherwise be caught as "unreachable" (exit 1).
    except requests.exceptions.JSONDecodeError as exc:
        log_error("bad_response", f"API response is not valid JSON: {exc}")
        return 2
    except requests.exceptions.Timeout:
        log_error("upstream_unreachable", f"Luchtmeetnet API timed out for station {STATION}")
        return 1
    except requests.exceptions.HTTPError as exc:
        log_error("upstream_unreachable", f"Luchtmeetnet API returned HTTP error: {exc}")
        return 1
    except requests.exceptions.RequestException as exc:
        log_error("upstream_unreachable", f"could not reach Luchtmeetnet API: {exc}")
        return 1
    except ValueError as exc:  # wrong shape or no measurements (from get_no2_measurements)
        log_error("bad_response", f"unusable API response: {exc}")
        return 2

    try:
        readings = records_to_frame(records)
    except ValueError as exc:
        log_error("bad_response", str(exc))
        return 2

    # The request already asks for NO2, but do not trust that blindly.
    no2 = filter_no2_readings(readings)
    if no2.empty:
        got = sorted(readings["component"].astype(str).unique())
        log_error("bad_response", f"expected formula {FORMULA}, got only {got}")
        return 2

    latest = no2.loc[no2["timestamp"].idxmax()]
    log.info(json.dumps({
        "event": "fetch_success", "source": SOURCE,
        "station_id": STATION,
        "value": None if latest["value_missing"] else float(latest["value"]),
        "timestamp": utc_z(latest["timestamp"].to_pydatetime()),
    }))
    last_successful_fetch = utc_z(datetime.now(timezone.utc))
    log.info(json.dumps({
        "event": "rows_fetched", "source": SOURCE, "station_id": STATION,
        "rows": len(no2),
        "null_values": int(no2["value_missing"].sum()),
        "missing_hours": missing_hours(no2["timestamp"]),
        "from": utc_z(no2["timestamp"].min().to_pydatetime()),
        "to": utc_z(no2["timestamp"].max().to_pydatetime()),
    }))

    # Data quality (task 005): stale or null rows are flagged and still written.
    no2["is_flagged"] = stale_or_null(no2)
    for row in no2[no2["is_flagged"]].itertuples(index=False):
        if row.timestamp in _reported_flagged:
            continue  # already reported by an earlier fetch of the same hour (D-040)
        _reported_flagged.add(row.timestamp)
        log.warning(json.dumps({
            "event": "DATA_QUALITY_ERROR", "source": SOURCE,
            "station_id": STATION, "field": FORMULA,
            "reason": "stale_or_null",
            "value": None if pd.isna(row.value) else float(row.value),
        }))
        record_bad_data()

    if store is None:
        return 0
    try:
        changed = store(no2)
    except psycopg2.Error as exc:
        # Unreachable host, refused login, missing table, failed insert: our own
        # storage failed, not the API (so not exit 1) and not the data (not 2).
        detail = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        log_error("store_failed", f"could not write to database: {detail}")
        return 3
    log.info(json.dumps({
        "event": "db_write", "source": SOURCE,
        "rows_sent": len(no2),
        "rows_changed": changed,  # inserted or newly flagged (D-039)
        "rows_unchanged": len(no2) - changed,
    }))
    return 0


# --- Long-running mode with GET /health (task 005, D-041) ----------------------
HEALTH_PORT = 8001


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        body = json.dumps({"last_successful_fetch": last_successful_fetch,
                           "bad_data_count": luchtmeetnet_bad_data_count,
                           "source": SOURCE}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # no plain-text access-log lines on stderr between the JSON events


def serve(run_once) -> None:
    """Serve /health in a background thread, call run_once() now and then at
    the start of every following hour. A failed run is logged, never fatal."""
    # 0.0.0.0 so Docker can forward the port; compose publishes it on 127.0.0.1 only.
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    while True:
        try:
            run_once()  # a non-zero exit code has already been logged as an ERROR event
        except Exception as exc:  # an unexpected bug must not stop the service
            log_error("run_failed", f"{type(exc).__name__}: {exc}")
        time.sleep(3600 - time.time() % 3600)  # until the next full hour


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Ingest NO2 readings for NL10240.")
    parser.add_argument("--no-db", action="store_true",
                        help="fetch and log only; do not write to the database")
    parser.add_argument("--serve", action="store_true",
                        help=f"run every hour and serve GET /health on port {HEALTH_PORT}")
    return parser.parse_args(argv)


if __name__ == "__main__":
    handler = logging.StreamHandler(sys.stdout)  # JSON lines on stdout (D-038)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)

    args = parse_args(sys.argv[1:])
    store = None
    if not args.no_db:
        try:
            # Checked before fetching, so a misconfigured run does not call the API.
            settings = db_settings_from_env()
        except ValueError as exc:
            log_error("store_failed", str(exc))
            sys.exit(3)
        store = lambda df: store_readings(df, settings)  # noqa: E731
    if args.serve:
        serve(lambda: main(store=store))
    sys.exit(main(store=store))
