"""Unit tests for src/ingest_air.py (airbreda/src/). HTTP is mocked: these tests never touch the network."""

import json
from unittest import mock

import pandas as pd
import requests

import ingest_air
from ingest_air import filter_no2_readings  # adjust to your actual function name

OK_BODY = {
    "data": [
        {"formula": "NO2", "value": 23.8, "timestamp_measured": "2026-09-30T10:00:00+00:00"},
        {"formula": "NO2", "value": None, "timestamp_measured": "2026-09-30T09:00:00+00:00"},
    ]
}


def make_response(status=200, body=None, text=None):
    """A real requests.Response, so raise_for_status() and .json() behave as in production."""
    resp = requests.Response()
    resp.status_code = status
    resp._content = (text if text is not None else json.dumps(body)).encode()
    resp.url = "https://api.luchtmeetnet.nl/open_api/stations/NL10240/measurements"
    return resp


def test_filter_no2_readings_handles_null_value():
    df = pd.DataFrame({
        "component": ["NO2", "NO2", "PM10"],
        "value": [18.4, None, 22.1],
        "timestamp": ["2024-01-15T08:00:00Z", "2024-01-15T09:00:00Z", "2024-01-15T08:00:00Z"],
    })
    result = filter_no2_readings(df)
    assert len(result) == 2
    assert result["value"].isnull().sum() == 1  # null NO2 rows are kept, not silently dropped


def test_retry_succeeds_after_one_timeout(caplog):
    sleeps = []
    with mock.patch(
        "getNO2Readings.requests.get",
        side_effect=[requests.exceptions.Timeout("read timed out"), make_response(body=OK_BODY)],
    ) as get:
        code = ingest_air.main(sleep=sleeps.append)

    assert code == 0
    assert get.call_count == 2
    assert sleeps == [1.0]  # one backoff, not actually slept
    out = caplog.text
    assert '"event": "fetch_success"' in out and '"value": 23.8' in out
    assert '"null_values": 1' in out  # the null row went through the real flow, kept


def test_persistent_outage_exits_1_after_retry_limit(caplog):
    sleeps = []
    with mock.patch(
        "getNO2Readings.requests.get", return_value=make_response(status=503, text="down")
    ) as get:
        code = ingest_air.main(sleep=sleeps.append)

    assert code == 1
    assert get.call_count == ingest_air.MAX_ATTEMPTS  # bounded: 3 requests, then give up
    assert sleeps == [1.0, 2.0]  # exponential backoff between attempts
    assert "503" in caplog.text


def test_bad_data_exits_2_without_retry(caplog):
    sleeps = []
    with mock.patch(
        "getNO2Readings.requests.get", return_value=make_response(text="<html>maintenance</html>")
    ) as get:
        code = ingest_air.main(sleep=sleeps.append)

    assert code == 2
    assert get.call_count == 1  # a retry cannot fix bad data
    assert sleeps == []
    assert "not valid JSON" in caplog.text
