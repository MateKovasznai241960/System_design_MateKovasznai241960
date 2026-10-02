"""
Ingest NDW traffic readings for the four A27 sites near NL10240 (Breda) into
CSV files, one per site per hour, and upload each file to the S3 bucket.

    local:  <data dir>/ndw/YYYY-MM-DD/HH-<site>.csv     (default data dir: ./data)
    S3:     s3://<bucket>/ndw/YYYY-MM-DD/HH-<site>.csv  (key = the same relative path)

Sites: hrl, hrr (A27 mainline, both directions), vwd, vwa (slip roads). The full
site IDs are TARGET_SITE_IDS in getTrafficReadings.py (there is no course README
in this repo, so that file is the mapping we use).

Date and hour come from the measurement period in the data (NDW's
measurementTimeDefault, which NDW documents as the START of the 1-minute
period), in UTC, never from the clock. UTC because the database stores
TIMESTAMPTZ/UTC too, and because local time repeats 02:00-02:59 on the last
Sunday of October (two different hours would get the same file name) and skips
an hour in March. Re-running within the same hour overwrites the same key
(last write wins), so a re-run never creates a duplicate file.

Task 005: the valid readings are also written to sensor_readings (D-043);
a speed of -1 is logged as a DATA_QUALITY_ERROR, counted and not written.
The CSV files keep every value unchanged. Output: one JSON object per line
on stdout (D-038). Database settings: the PG* variables, as in ingest_air.py.

Usage:
    python ingest_traffic.py --no-upload                 # parse + save locally + database, no S3
    python ingest_traffic.py --bucket airbreda-xyz       # + upload (or set AIRBREDA_BUCKET)
    python ingest_traffic.py --serve                     # long-running: every hour, GET /health on port 8002 (D-041)
    options: --profile (or AWS_PROFILE), --region (or AWS_REGION), --data-dir

Exit codes (same meaning as ingest_air.py, D-003 / D-020):
    0  success
    1  NDW download failed (timeout, connection/DNS error, HTTP error status)
    2  bad data: not gzip, not valid XML, no readings for any site, a reading
       without a timestamp or with a non-numeric value, or a value whose index
       is not in the site configuration
    3  could not store the result: no bucket configured, AWS profile or
       credentials missing, bucket not reachable, local write or S3 upload
       failed, database settings missing or the database write failed

Why do we need BOTH the database (RDS PostgreSQL) AND the bucket (S3)?
-----------------------------------------------------------------------
They do different jobs, so neither can replace the other.

The bucket is the HISTORY layer. It keeps every hourly file this script has
ever written: the per-lane flow and speed values for our four sites, with the
values unchanged (a -1 "no data" speed stays -1). It is append-only history,
not curated and not deduplicated, and no later step edits it. It holds the
PARSER'S OUTPUT, not the source: the two national XML files (about 147 MB +
70 MB unpacked, ~220 MB in total) are not kept, and the reference parser
(build_index_map / extract_measurements) already drops details such as the
vehicle-length class and NDW's accuracy attribute.
It is schema-on-read for every step after this script: this script validates
each site before writing (site_rows raises BadData and nothing is written),
but S3 itself enforces no schema, and cleaning, aggregation and features are
decided by whoever reads the files, so they can be decided again later. It is
cheap per GB. What it cannot do: it has no query engine, no joins or indexes,
and no transactions across objects, so "average flow per hour last week"
means listing, reading and parsing many files. A key gives last-write-wins (a
re-run in the same hour replaces that hour's file), not a DB-style uniqueness
constraint across rows.

The database is the CURATED layer. The table has a fixed, typed schema that is
checked on write (schema-on-write) and a primary key, so a reading is stored
once. With ON CONFLICT DO NOTHING, re-runs are idempotent and the table holds
the first-seen value of each reading: a deduplicated, queryable current state
that answers SQL in milliseconds. It holds NO2 and, since task 005, the
valid NDW readings of the sampled minute (D-043). What it cannot do:
give back anything we did not load (columns we dropped, the raw payload, a
later value skipped because the row already existed). A bug at ingest is
baked into its rows.

Retraining the ML model six months from now:
- NO2 comes from the database: one SQL query returns typed, deduplicated
  hourly rows (first-seen, provisional values, D-019). If validated values
  are needed, RIVM keeps its own history, which can be re-fetched (within
  fair use).
- Traffic comes from the bucket: the training step reads the ndw/... CSVs
  (4 sites x 24 h x ~180 days, about 17,000 files), cleans them (e.g. drops
  -1 speeds), aggregates per site and UTC hour, and joins them with NO2 on
  the UTC hour (e.g. in pandas). Since task 005 the same minute is also in
  the database (station_id NDW_<site>, component flow_<index>/speed_<index>,
  D-043), but only from task 005 on and without the -1 speeds.
- Because the history is kept, a new cleaning rule, a new feature (per-lane
  flow, speed spread, slip-road share) or a fix in any step after the CSV is
  applied by recomputing from the stored files and retraining. The NDW live
  feed cannot give old traffic back: it only holds the current minute and is
  replaced every minute, so six-month-old traffic exists only in our bucket.
- What the bucket cannot fix: a bug in build_index_map / extract_measurements
  (the XML-to-CSV step) or a field that step drops, because the source XML is
  not kept. Nor the other 59 minutes of each hour: one file holds one minute.
In short: the bucket keeps the history we can recompute from; the database
gives the curated state we can query.
"""

import argparse
import csv
import gzip
import http.client
import json
import logging
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
import zlib
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import boto3
import botocore.exceptions
import psycopg2
from boto3.exceptions import S3UploadFailedError
from psycopg2.extras import execute_values

# Reused from the reference script, not rewritten (task 003).
from getTrafficReadings import (
    CONFIG_URL,
    MEASURED_URL,
    TARGET_SITE_IDS,
    build_index_map,
    download_and_decompress,
    extract_measurements,
)

SITE_CODES = ("hrl", "hrr", "vwd", "vwa")
# Short code -> full NDW site ID, taken from getTrafficReadings.TARGET_SITE_IDS
# so the IDs live in one place.
SITES = {code: next(sid for sid in TARGET_SITE_IDS if code in sid) for code in SITE_CODES}

CSV_COLUMNS = [
    "site",              # short code: hrl / hrr / vwd / vwa
    "site_id",           # full NDW measurement site ID
    "period_start_utc",  # NDW measurementTimeDefault: start of the measurement minute
    "index",             # NDW measurement index within the site (flow and speed per lane)
    "quantity",          # trafficFlow or trafficSpeed (from the site configuration)
    "vehicle_type",      # anyVehicle, or specific-class (a vehicle-length class)
    "value",             # exactly as NDW sent it (raw, e.g. -1 speed = no valid value)
    "unit",
]
UNITS = {"trafficFlow": "vehicles/hour", "trafficSpeed": "km/h"}

# --- Structured logging (task 005, D-038): same set-up as ingest_air.py --------
SOURCE = "NDW"
log = logging.getLogger("ingest_traffic")
log.setLevel(logging.INFO)

# --- Data quality counter and /health state (task 005, D-041, D-042) -----------
ndw_bad_data_count = 0       # +1 for every DATA_QUALITY_ERROR WARNING logged
last_successful_fetch = None  # UTC time of the last successful fetch, "...Z"
THRESHOLD = 10                # more than 10 warnings in a rolling hour -> one ERROR event
WINDOW_S = 3600.0
_warning_times = deque()      # time.monotonic() of each warning still in the window
_threshold_logged = False

# --- sensor_readings mapping for NDW rows (task 005, D-043) --------------------
# The table has no lane column and its key is (station_id, timestamp,
# component), while each site has several readings per minute (one per NDW
# index). station_id = "NDW_" + site code (7 chars, fits VARCHAR(20); the full
# 27-char NDW ID does not). component = quantity + "_" + index, e.g. flow_1 or
# speed_2 (fits VARCHAR(10) up to index 9999), so every reading keeps its own
# key and no lane is dropped by the primary key.
COMPONENT_PREFIX = {"trafficFlow": "flow", "trafficSpeed": "speed"}
INSERT_SQL = """
    INSERT INTO sensor_readings (station_id, timestamp, component, value)
    VALUES %s
    ON CONFLICT (station_id, timestamp, component) DO NOTHING
    RETURNING 1
"""
DB_REQUIRED_ENV = ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD")
DB_CONNECT_TIMEOUT_S = 10


def db_settings_from_env(env=os.environ) -> dict:
    """Same as ingest_air.db_settings_from_env (D-021). Copied, not imported:
    importing ingest_air would pull pandas and requests into this image."""
    missing = [name for name in DB_REQUIRED_ENV if not env.get(name)]
    if missing:
        raise ValueError(f"database settings missing: set {', '.join(missing)}")
    return {
        "host": env["PGHOST"],
        "port": int(env.get("PGPORT", "5432")),
        "dbname": env["PGDATABASE"],
        "user": env["PGUSER"],
        "password": env["PGPASSWORD"],
        "sslmode": env.get("PGSSLMODE", "require"),
        "connect_timeout": DB_CONNECT_TIMEOUT_S,
    }


def store_rows(rows, settings: dict) -> int:
    """Insert the rows in one transaction; return how many were new."""
    conn = psycopg2.connect(**settings)
    try:
        with conn:  # commit on success, roll back on any exception
            with conn.cursor() as cur:
                inserted = execute_values(cur, INSERT_SQL, rows, fetch=True)
    finally:
        conn.close()
    return len(inserted)


def record_bad_data(clock=time.monotonic) -> None:
    """Count one DATA_QUALITY_ERROR and apply the rolling-hour threshold (D-042).
    Same rule as ingest_air.record_bad_data."""
    global ndw_bad_data_count, _threshold_logged
    ndw_bad_data_count += 1
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


def db_rows(parsed) -> list:
    """sensor_readings tuples for the valid readings of all parsed sites.

    A speed of -1 (NDW: no valid value) is logged as a DATA_QUALITY_ERROR,
    counted and skipped, so the sentinel never reaches the table (task 005).
    """
    rows = []
    for code, period_start, site in parsed:
        for r in site:
            if r["quantity"] == "trafficSpeed" and float(r["value"]) == -1:
                log.warning(json.dumps({
                    "event": "DATA_QUALITY_ERROR", "source": SOURCE,
                    "location": r["site_id"], "field": "speed", "value": -1,
                }))
                record_bad_data()
                continue
            prefix = COMPONENT_PREFIX.get(r["quantity"])
            if prefix is None:
                raise BadData(f"{code}: quantity {r['quantity']} has no sensor_readings component")
            rows.append((f"NDW_{code}", period_start, f"{prefix}_{r['index']}", float(r["value"])))
    return rows


def log_error(event: str, error: str) -> None:
    """One ERROR event. `event` names the exit-code meaning (D-003/D-020)."""
    log.error(json.dumps({"event": event, "source": SOURCE, "error": error}))


class BadData(Exception):
    """The feed was downloaded but its content is unusable (exit 2)."""


def parse_period_start(raw) -> datetime:
    """NDW timeValue (e.g. '2026-09-30T11:13:00Z') -> aware UTC datetime."""
    if not raw:
        raise BadData("reading has no measurement time")
    try:
        ts = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise BadData(f"unparseable measurement time {raw!r}") from exc
    if ts.tzinfo is None:
        raise BadData(f"measurement time has no UTC offset: {raw!r}")
    return ts.astimezone(timezone.utc)


def site_rows(code, site_id, config, measured):
    """Parse one site from the two in-memory XML files into CSV rows.

    Returns (period_start, rows); rows is empty if NDW has no readings for the
    site right now. Raises BadData for content we cannot trust.
    """
    config.seek(0)
    index_map = build_index_map(config, site_id)
    measured.seek(0)
    readings = extract_measurements(measured, site_id)
    if not readings:
        return None, []

    period_start = parse_period_start(readings[0].get("timestamp"))
    rows = []
    for r in readings:
        if parse_period_start(r.get("timestamp")) != period_start:
            raise BadData(f"{code}: readings with different measurement times")
        info = index_map.get(r["index"])
        if info is None:
            raise BadData(f"{code}: index {r['index']} is not in the site configuration")
        # The measured file says TrafficFlow/TrafficSpeed, the config says
        # trafficFlow/trafficSpeed: they must describe the same quantity.
        if (r["type"] or "").lower() != info["type"].lower():
            raise BadData(f"{code}: index {r['index']} is {r['type']} in the data "
                          f"but {info['type']} in the configuration")
        try:
            float(r["value"])
        except (TypeError, ValueError) as exc:
            raise BadData(f"{code}: non-numeric value {r['value']!r}") from exc
        rows.append({
            "site": code,
            "site_id": site_id,
            "period_start_utc": period_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "index": r["index"],
            "quantity": info["type"],
            "vehicle_type": info["vehicle"],
            "value": r["value"],
            "unit": UNITS.get(info["type"], ""),
        })
    rows.sort(key=lambda row: int(row["index"]))
    return period_start, rows


def relative_path(code, period_start) -> Path:
    """ndw/YYYY-MM-DD/HH-<code>.csv from the measurement period (UTC)."""
    return Path("ndw") / f"{period_start:%Y-%m-%d}" / f"{period_start:%H}-{code}.csv"


def write_csv(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv):
    p = argparse.ArgumentParser(description="Ingest NDW traffic for the A27 sites near Breda.")
    p.add_argument("--no-upload", action="store_true", help="save locally only, do not upload")
    p.add_argument("--bucket", default=os.environ.get("AIRBREDA_BUCKET"),
                   help="S3 bucket name (default: $AIRBREDA_BUCKET)")
    p.add_argument("--profile", default=os.environ.get("AWS_PROFILE"),
                   help="AWS CLI profile (default: $AWS_PROFILE)")
    p.add_argument("--region", default=os.environ.get("AWS_REGION"),
                   help="AWS region (default: $AWS_REGION)")
    p.add_argument("--data-dir", type=Path, default=Path(os.environ.get("AIRBREDA_DATA_DIR", "data")),
                   help="local output directory (default: ./data)")
    p.add_argument("--serve", action="store_true",
                   help=f"run every hour and serve GET /health on port {HEALTH_PORT}")
    return p.parse_args(argv)


AWS_ERRORS = (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError, S3UploadFailedError)


def main(argv=None, store=None) -> int:
    """Download, parse, save/upload the CSVs; then call store(rows) if given.

    `store` is injected by the command-line entry point below, like in
    ingest_air.py. Tests call main() without a database.
    """
    global last_successful_fetch
    args = parse_args(sys.argv[1:] if argv is None else argv)

    # 1. Storage settings first, so a misconfigured run does not download ~3 MB
    #    (220 MB unpacked) for nothing. head_bucket is a read-only check.
    s3 = None
    if not args.no_upload:
        if not args.bucket:
            log_error("store_failed", "no bucket configured: pass --bucket or set AIRBREDA_BUCKET "
                      "(or use --no-upload)")
            return 3
        try:
            session = boto3.Session(profile_name=args.profile, region_name=args.region)
            s3 = session.client("s3")
            s3.head_bucket(Bucket=args.bucket)
        except AWS_ERRORS as exc:
            log_error("store_failed", f"cannot use bucket {args.bucket}: {exc}")
            return 3

    # 2. Download both feeds (reused function: whole file into memory, then
    #    gunzip; the XML itself is stream-parsed with iterparse below).
    try:
        log.info(json.dumps({"event": "download_started", "source": SOURCE, "url": CONFIG_URL}))
        config = download_and_decompress(CONFIG_URL)
        log.info(json.dumps({"event": "download_started", "source": SOURCE, "url": MEASURED_URL}))
        measured = download_and_decompress(MEASURED_URL)
    # Must come first: BadGzipFile is a subclass of OSError, like the network errors.
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        log_error("bad_response", f"NDW file is not valid gzip: {exc}")
        return 2
    except (OSError, http.client.HTTPException) as exc:  # URLError, HTTPError, timeouts
        log_error("upstream_unreachable", f"could not download from NDW: {exc}")
        return 1

    # 3. Parse each site. A site with no readings right now is missing data,
    #    not bad data (D-012): warn and skip. No readings at all means the feed
    #    changed, so exit 2.
    parsed = []
    try:
        for code, site_id in SITES.items():
            period_start, rows = site_rows(code, site_id, config, measured)
            if not rows:
                log.warning(json.dumps({"event": "no_readings", "source": SOURCE,
                                        "location": site_id, "error": "no readings in this feed; no file written"}))
                continue
            parsed.append((code, period_start, rows))
    except ET.ParseError as exc:
        log_error("bad_response", f"NDW file is not valid XML: {exc}")
        return 2
    except BadData as exc:
        log_error("bad_response", f"unusable NDW data: {exc}")
        return 2
    if not parsed:
        log_error("bad_response", "unusable NDW data: none of the four sites has readings")
        return 2
    for code, period_start, rows in parsed:  # one event per site fetched successfully
        log.info(json.dumps({"event": "fetch_success", "source": SOURCE, "location": SITES[code],
                             "timestamp": period_start.strftime("%Y-%m-%dT%H:%M:%SZ")}))
    last_successful_fetch = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. Save locally, then upload each file under the same relative path.
    saved = []
    try:
        for code, period_start, rows in parsed:
            rel = relative_path(code, period_start)
            write_csv(args.data_dir / rel, rows)
            saved.append(rel)
            log.info(json.dumps({"event": "file_saved", "source": SOURCE,
                                 "path": (args.data_dir / rel).as_posix(), "values": len(rows),
                                 "period_start": period_start.strftime("%Y-%m-%dT%H:%M:%SZ")}))
    except OSError as exc:
        log_error("store_failed", f"could not write local file: {exc}")
        return 3

    if args.no_upload:
        log.info(json.dumps({"event": "upload_skipped", "source": SOURCE, "reason": "--no-upload"}))
    else:
        for rel in saved:
            key = rel.as_posix()  # S3 key mirrors the local path; same period -> same key
            try:
                s3.upload_file(str(args.data_dir / rel), args.bucket, key,
                               ExtraArgs={"ContentType": "text/csv"})
            except AWS_ERRORS as exc:
                log_error("store_failed", f"upload of {key} failed: {exc}")
                return 3
            log.info(json.dumps({"event": "file_uploaded", "source": SOURCE,
                                 "uri": f"s3://{args.bucket}/{key}"}))

    # 5. Task 005: the valid readings also go to sensor_readings (D-043). The
    #    CSVs above keep every value; -1 speeds are logged and skipped here.
    try:
        rows = db_rows(parsed)
    except BadData as exc:
        log_error("bad_response", f"unusable NDW data: {exc}")
        return 2
    if store is None:
        return 0
    try:
        inserted = store(rows)
    except psycopg2.Error as exc:
        detail = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        log_error("store_failed", f"could not write to database: {detail}")
        return 3
    log.info(json.dumps({"event": "db_write", "source": SOURCE, "rows_sent": len(rows),
                         "rows_changed": inserted, "rows_unchanged": len(rows) - inserted}))
    return 0


# --- Long-running mode with GET /health (task 005, D-041) ----------------------
# Same ~30 lines as in ingest_air.py (each script stays self-contained).
HEALTH_PORT = 8002


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        body = json.dumps({"last_successful_fetch": last_successful_fetch,
                           "bad_data_count": ndw_bad_data_count,
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


if __name__ == "__main__":
    handler = logging.StreamHandler(sys.stdout)  # JSON lines on stdout (D-038)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)

    cli = parse_args(sys.argv[1:])
    try:
        # Checked before downloading, like the bucket check (D-020).
        settings = db_settings_from_env()
    except ValueError as exc:
        log_error("store_failed", str(exc))
        sys.exit(3)
    store = lambda rows: store_rows(rows, settings)  # noqa: E731
    if cli.serve:
        serve(lambda: main(store=store))
    sys.exit(main(store=store))
