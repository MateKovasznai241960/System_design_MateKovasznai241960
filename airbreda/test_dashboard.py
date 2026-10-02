"""Task 008: the dashboard routes, without a database, a bucket or the ingest containers.

latest_no2() is replaced by a stub and the S3 client by a small fake that holds
CSV files in the same layout and columns ingest_traffic.py uploads.
"""

import io
import json
import urllib.error
from datetime import datetime, timezone

from fastapi.testclient import TestClient

import dashboard

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
HEADER = "site,site_id,period_start_utc,index,quantity,vehicle_type,value,unit\n"


def csv_text(period_start, rows):
    lines = [f"hrl,RWS01_MONIBAS_0271hrl0631ra,{period_start},{i},{q},anyVehicle,{v},u\n"
             for i, (q, v) in enumerate(rows, start=1)]
    return HEADER + "".join(lines)


class FakeS3:
    def __init__(self, files):
        self.files = files  # key -> CSV text

    def list_objects_v2(self, Bucket, Prefix):
        return {"Contents": [{"Key": k} for k in self.files if k.startswith(Prefix)]}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.files[Key].encode())}


def setup(monkeypatch):
    monkeypatch.setattr(dashboard, "latest_no2",
                        lambda: (18.4, datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)))
    monkeypatch.setattr(dashboard, "s3", FakeS3({
        f"ndw/{TODAY}/08-hrl.csv": csv_text("2026-10-01T08:00:00Z", [("trafficFlow", 9999)]),
        # The newest file: 1200 + 2000 vehicles/hour; the speed and the -1 flow are left out.
        f"ndw/{TODAY}/09-hrl.csv": csv_text("2026-10-01T09:12:00Z", [
            ("trafficFlow", 1200), ("trafficSpeed", 95), ("trafficFlow", 2000), ("trafficFlow", -1)]),
    }))
    return TestClient(dashboard.app)


def test_site_returns_readings_and_prediction(monkeypatch):
    calls = []
    monkeypatch.setattr(dashboard, "predict", lambda intensity, hour: calls.append((intensity, hour))
                        or {"no2_ug_m3_predicted": 25.9, "no2_exceedance_risk": 0.05})
    body = setup(monkeypatch).get("/site/hrl").json()
    assert calls == [(3200.0, 9)]  # site intensity, UTC hour of the traffic measurement
    assert body == {"site_id": "hrl", "no2_ug_m3": 18.4, "intensity_veh_per_hr": 3200.0,
                    "no2_ug_m3_predicted": 25.9, "no2_exceedance_risk": 0.05,
                    "timestamp": "2026-10-01T09:00:00Z"}  # the older of the two input times


def test_site_with_the_real_model(monkeypatch):
    body = setup(monkeypatch).get("/site/hrl").json()
    assert 0 <= body["no2_exceedance_risk"] <= 1


def test_unknown_site_is_404(monkeypatch):
    assert setup(monkeypatch).get("/site/abc").status_code == 404


def test_site_degrades_when_predict_raises(monkeypatch):
    def broken(intensity, hour):
        raise RuntimeError("model.pkl missing")
    monkeypatch.setattr(dashboard, "predict", broken)
    response = setup(monkeypatch).get("/site/hrl")
    assert response.status_code == 200
    body = response.json()
    assert (body["no2_ug_m3"], body["intensity_veh_per_hr"]) == (18.4, 3200.0)
    assert body["no2_ug_m3_predicted"] is None and body["no2_exceedance_risk"] is None


def test_health_aggregates_both_sources(monkeypatch):
    answers = {"air-ingest": {"last_successful_fetch": "2026-10-01T09:00:05Z",
                              "bad_data_count": 2, "source": "Luchtmeetnet"},
               "traffic-ingest": {"last_successful_fetch": "2026-10-01T09:00:41Z",
                                  "bad_data_count": 5, "source": "NDW"}}

    def fake_urlopen(url, timeout):
        for host, body in answers.items():
            if host in url:
                if body is None:
                    raise urllib.error.URLError(f"{host} not reachable")
                return io.BytesIO(json.dumps(body).encode())
    monkeypatch.setattr(dashboard.urllib.request, "urlopen", fake_urlopen)
    client = setup(monkeypatch)
    assert client.get("/health").json() == {
        "status": "ok",
        "luchtmeetnet": {"last_successful_fetch": "2026-10-01T09:00:05Z", "bad_data_count": 2},
        "ndw": {"last_successful_fetch": "2026-10-01T09:00:41Z", "bad_data_count": 5},
    }
    answers["traffic-ingest"] = None  # one service down: same shape, nulls, status degraded
    body = client.get("/health").json()
    assert body["status"] == "degraded"
    assert body["ndw"] == {"last_successful_fetch": None, "bad_data_count": None}


def test_successes_and_failures_are_logged_as_json(monkeypatch, caplog):
    client = setup(monkeypatch)
    caplog.set_level("INFO", logger="dashboard")
    assert client.get("/site/hrl").status_code == 200

    def db_down():
        raise dashboard.psycopg2.OperationalError("could not connect to server")
    monkeypatch.setattr(dashboard, "latest_no2", db_down)
    assert client.get("/site/hrl").status_code == 500
    events = [json.loads(r.getMessage()) for r in caplog.records if r.name == "dashboard"]
    names = [e["event"] for e in events]
    assert "site_served" in names and "db_error" in names
    statuses = [e["status"] for e in events if e["event"] == "request"]
    assert statuses == [200, 500]


def test_page_calls_the_four_routes(monkeypatch):
    response = setup(monkeypatch).get("/")
    assert response.headers["content-type"].startswith("text/html")
    for site_id in ("hrl", "hrr", "vwd", "vwa"):
        assert f'fetch("/site/{site_id}")' in response.text
