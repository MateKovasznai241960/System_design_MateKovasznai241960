"""AirBreda dashboard (task 008): one FastAPI app with three routes.

    GET /site/{site_id}  latest real NO2, the site's latest traffic intensity and the model's prediction
    GET /health          both ingest services' /health, in one response
    GET /                HTML page that calls /site/hrl, /site/hrr, /site/vwd and /site/vwa

Run: uvicorn dashboard:app --host 0.0.0.0 --port 8000 (the Dockerfile.dashboard CMD).
Settings come only from environment variables (the .env file through Compose):
PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE (D-021) for the database,
AIRBREDA_BUCKET, AWS_REGION and the AWS credentials for the bucket.
"""

import csv
import io
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from predict import predict

SITES = ("hrl", "hrr", "vwd", "vwa")
STATION = "NL10240"

# Structured logging, as in the two ingest scripts (D-038): one named logger, one JSON event
# per line on stdout. uvicorn imports this module rather than running it, so the handler is
# attached here instead of under __main__. uvicorn's own plain-text access log is switched
# off; the middleware below logs every request as a JSON "request" event instead.
log = logging.getLogger("dashboard")
log.setLevel(logging.INFO)
if not log.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(_handler)
logging.getLogger("uvicorn.access").disabled = True

app = FastAPI(title="AirBreda dashboard")


def log_event(level, event, **fields):
    log.log(level, json.dumps({"event": event, "source": "dashboard", **fields}))


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.monotonic()
    try:
        response = await call_next(request)
    except Exception as exc:  # not raised by our routes today; logged so nothing fails silently
        log_event(logging.ERROR, "request_failed", method=request.method,
                  path=request.url.path, error=repr(exc))
        raise
    status = response.status_code
    level = logging.ERROR if status >= 500 else logging.WARNING if status >= 400 else logging.INFO
    log_event(level, "request", method=request.method, path=request.url.path, status=status,
              duration_ms=round((time.monotonic() - start) * 1000, 1))
    return response
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))


def latest_no2():
    """(value, timestamp) of the newest non-null NO2 row for NL10240, or None.

    libpq reads PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD from the
    environment itself, so no setting is copied into this code. sslmode keeps
    the project default "require" (D-021) unless PGSSLMODE says otherwise.
    """
    conn = psycopg2.connect(sslmode=os.environ.get("PGSSLMODE", "require"), connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value, timestamp FROM sensor_readings"
                " WHERE station_id = %s AND component = 'NO2' AND value IS NOT NULL"
                " ORDER BY timestamp DESC LIMIT 1",
                (STATION,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def latest_intensity(site_id):
    """(intensity in veh/h, period start UTC) from the site's newest CSV in the bucket, or None.

    ingest_traffic.py uploads one CSV per site per hour as ndw/YYYY-MM-DD/HH-<site>.csv
    (UTC). Only today's and yesterday's folders are listed, so a page load costs
    one or two LIST requests however long the history grows. Intensity = the sum
    of the site's trafficFlow values over all lanes, with NDW's -1 ("no value")
    left out: the same rule task 007 used for total_intensity_veh_per_hr, per site.
    """
    now = datetime.now(timezone.utc)
    for day in (now, now - timedelta(days=1)):
        listing = s3.list_objects_v2(Bucket=os.environ.get("AIRBREDA_BUCKET"), Prefix=f"ndw/{day:%Y-%m-%d}/")
        keys = [obj["Key"] for obj in listing.get("Contents", []) if obj["Key"].endswith(f"-{site_id}.csv")]
        if keys:
            body = s3.get_object(Bucket=os.environ.get("AIRBREDA_BUCKET"), Key=max(keys))["Body"].read()
            rows = list(csv.DictReader(io.StringIO(body.decode("utf-8"))))
            intensity = sum(float(r["value"]) for r in rows
                            if r["quantity"] == "trafficFlow" and float(r["value"]) >= 0)
            period_start = datetime.fromisoformat(rows[0]["period_start_utc"])
            return intensity, period_start
    return None


# Where each field of the /site/{site_id} response comes from:
#   site_id               the URL path; only hrl, hrr, vwd and vwa are accepted (404 otherwise).
#   no2_ug_m3             the database: the newest non-null NO2 value for station NL10240 in
#                         sensor_readings (latest_no2). The same real reading for every site.
#   intensity_veh_per_hr  the bucket: the sum of the site's trafficFlow values in its newest
#                         ndw/.../HH-<site>.csv (latest_intensity).
#   no2_ug_m3_predicted   predict() in predict.py (model.pkl baked into the image), called with
#   no2_exceedance_risk   intensity_veh_per_hr and hour_of_day = the UTC hour of the traffic
#                         measurement, the same UTC hour task 007 trained on. Predicted NO2 is
#                         returned so the page can show it without computing anything itself.
#   timestamp             the OLDER of the two input times (the NO2 timestamp, which is the end
#                         of its hourly average, and the traffic period start), in UTC. The page
#                         shows it as "last updated", so a stopped ingest service is visible.
# If predict() raises, the route does NOT fail: it logs the error and returns the real
# no2_ug_m3 and intensity_veh_per_hr with no2_ug_m3_predicted and no2_exceedance_risk set to
# null. The two real readings are still true and are what a person needs most; a broken model
# should not hide them. If the database or the bucket fails, the route does fail (500), and if
# either has no reading yet it returns 503: without the real readings there is nothing to show.
@app.get("/site/{site_id}")
def site(site_id: str):
    if site_id not in SITES:
        raise HTTPException(status_code=404, detail=f"unknown site {site_id!r}; use one of {', '.join(SITES)}")
    try:
        no2 = latest_no2()
    except psycopg2.Error as exc:
        log_event(logging.ERROR, "db_error", site_id=site_id, error=str(exc).strip())
        raise HTTPException(status_code=500, detail="could not read sensor_readings")
    if no2 is None:
        log_event(logging.WARNING, "no_reading", site_id=site_id, store="database")
        raise HTTPException(status_code=503, detail="no NO2 reading in sensor_readings yet")
    try:
        traffic = latest_intensity(site_id)
    except (BotoCoreError, ClientError) as exc:
        log_event(logging.ERROR, "bucket_error", site_id=site_id, error=str(exc))
        raise HTTPException(status_code=500, detail="could not read the traffic bucket")
    if traffic is None:
        log_event(logging.WARNING, "no_reading", site_id=site_id, store="bucket")
        raise HTTPException(status_code=503, detail=f"no traffic file for {site_id} in the bucket today or yesterday")
    no2_value, no2_time = no2
    intensity, traffic_time = traffic
    try:
        prediction = predict(intensity, traffic_time.hour)
    except Exception as exc:
        log_event(logging.ERROR, "predict_failed", site_id=site_id, error=repr(exc))
        prediction = {"no2_ug_m3_predicted": None, "no2_exceedance_risk": None}
    oldest = min(no2_time.astimezone(timezone.utc), traffic_time.astimezone(timezone.utc))
    log_event(logging.INFO, "site_served", site_id=site_id, no2_ug_m3=no2_value,
              intensity_veh_per_hr=intensity,
              no2_exceedance_risk=prediction["no2_exceedance_risk"])
    return {
        "site_id": site_id,
        "no2_ug_m3": no2_value,
        "intensity_veh_per_hr": intensity,
        "no2_ug_m3_predicted": prediction["no2_ug_m3_predicted"],
        "no2_exceedance_risk": prediction["no2_exceedance_risk"],
        "timestamp": oldest.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# Both ingest containers already serve /health (task 005, D-041). This route asks them over
# the Compose network by service name and reports both sources side by side, in the shape the
# assignment gives: {"status", "luchtmeetnet": {...}, "ndw": {...}}. "status" is "ok" only when
# both services answered and both have fetched successfully at least once; otherwise it is
# "degraded" and the missing values are null. It always answers 200: it reports, it does not
# judge (as the ingest /health does).
INGEST_HEALTH = (("luchtmeetnet", "http://air-ingest:8001/health"),
                 ("ndw", "http://traffic-ingest:8002/health"))


@app.get("/health")
def health():
    result = {"status": "ok"}
    for key, url in INGEST_HEALTH:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                body = json.load(response)
            result[key] = {"last_successful_fetch": body.get("last_successful_fetch"),
                           "bad_data_count": body.get("bad_data_count")}
        except (OSError, ValueError) as exc:  # unreachable, timeout, or not JSON
            log_event(logging.WARNING, "ingest_unreachable", target=key, url=url, error=str(exc))
            result[key] = {"last_successful_fetch": None, "bad_data_count": None}
        if result[key]["last_successful_fetch"] is None:
            result["status"] = "degraded"
    return result


# The page is only a client of /site/{id}: it shows what the API returns and computes nothing
# except the total traffic (the sum of the four intensities). It re-runs the four fetch calls
# every 5 minutes, so new hourly readings appear without a reload or a redeploy.
PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>AirBreda</title>
</head>
<body>
<h1>AirBreda: NO2 and traffic at the A27 near Breda</h1>
<p>Actual NO2 (station NL10240): <b id="no2">loading</b> &micro;g/m&sup3;</p>
<p>Total traffic (sum of the four sites): <b id="total">loading</b> vehicles/hour</p>
<table border="1" cellpadding="4">
<thead><tr><th>Site</th><th>Intensity (veh/h)</th><th>Predicted NO2 (&micro;g/m&sup3;)</th><th>Exceedance risk</th></tr></thead>
<tbody id="sites"></tbody>
</table>
<p>Last updated: <span id="updated">loading</span></p>
<script>
function show(value, digits) {
  return value === null ? "no prediction" : value.toFixed(digits);
}

async function refresh() {
  const responses = await Promise.all([
    fetch("/site/hrl"), fetch("/site/hrr"), fetch("/site/vwd"), fetch("/site/vwa")
  ]);
  const failed = responses.find(r => !r.ok);
  if (failed) {
    document.getElementById("updated").textContent = "error: HTTP " + failed.status + " from " + failed.url;
    return;
  }
  const data = await Promise.all(responses.map(r => r.json()));
  document.getElementById("no2").textContent = data[0].no2_ug_m3.toFixed(1);
  const total = data.reduce((sum, d) => sum + d.intensity_veh_per_hr, 0);
  document.getElementById("total").textContent = total.toFixed(0);
  const body = document.getElementById("sites");
  body.replaceChildren();
  for (const d of data) {
    const row = body.insertRow();
    row.insertCell().textContent = d.site_id;
    row.insertCell().textContent = d.intensity_veh_per_hr.toFixed(0);
    row.insertCell().textContent = show(d.no2_ug_m3_predicted, 1);
    row.insertCell().textContent = show(d.no2_exceedance_risk, 2);
  }
  // The oldest timestamp of the four responses: if an ingest service stopped, this stops moving.
  document.getElementById("updated").textContent = data.map(d => d.timestamp).sort()[0] + " (UTC)";
}

refresh();
setInterval(refresh, 5 * 60 * 1000);
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE
