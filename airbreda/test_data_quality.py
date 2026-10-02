"""Task 005: the two data-quality handlers. No database and no network: the
Luchtmeetnet API and the NDW downloads are faked, and storage is injected."""

import io
import json
from unittest import mock

import requests

import ingest_air
import ingest_traffic


def test_stale_or_null_luchtmeetnet_reading_is_written_flagged_not_dropped():
    # 3 consecutive hours with the same value (stale) and 1 null hour.
    body = {"data": [
        {"formula": "NO2", "value": None, "timestamp_measured": "2026-09-30T13:00:00+00:00"},
        {"formula": "NO2", "value": 18.4, "timestamp_measured": "2026-09-30T12:00:00+00:00"},
        {"formula": "NO2", "value": 18.4, "timestamp_measured": "2026-09-30T11:00:00+00:00"},
        {"formula": "NO2", "value": 18.4, "timestamp_measured": "2026-09-30T10:00:00+00:00"},
        {"formula": "NO2", "value": 25.0, "timestamp_measured": "2026-09-30T09:00:00+00:00"},
    ]}
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(body).encode()
    written = []  # what would go into the sensor_readings INSERT

    def fake_store(df):
        written.extend(ingest_air.frame_to_rows(df))
        return len(df)

    with mock.patch("getNO2Readings.requests.get", return_value=resp):
        assert ingest_air.main(sleep=lambda s: None, store=fake_store) == 0

    flags = {(ts.hour, value): is_flagged for _, ts, _, value, is_flagged in written}
    assert len(written) == 5  # nothing dropped
    assert flags == {(13, None): True, (12, 18.4): True, (11, 18.4): True, (10, 18.4): True,
                     (9, 25.0): False}


CONFIG_XML = b"""<root><measurementSite id="RWS01_MONIBAS_0271hrl0063ra">
<measurementSpecificCharacteristics index="1"><specificMeasurementValueType>trafficFlow</specificMeasurementValueType><vehicleType>anyVehicle</vehicleType></measurementSpecificCharacteristics>
<measurementSpecificCharacteristics index="2"><specificMeasurementValueType>trafficSpeed</specificMeasurementValueType><vehicleType>anyVehicle</vehicleType></measurementSpecificCharacteristics>
</measurementSite></root>"""

MEASURED_XML = b"""<root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><siteMeasurements>
<measurementSiteReference id="RWS01_MONIBAS_0271hrl0063ra"/>
<measurementTimeDefault><timeValue>2026-09-30T10:00:00Z</timeValue></measurementTimeDefault>
<physicalQuantity index="1"><basicData xsi:type="TrafficFlow"><vehicleFlowRate>600</vehicleFlowRate></basicData></physicalQuantity>
<physicalQuantity index="2"><basicData xsi:type="TrafficSpeed"><speed>-1</speed></basicData></physicalQuantity>
</siteMeasurements></root>"""


def test_ndw_speed_minus_one_is_not_written_and_is_counted(tmp_path):
    feeds = {ingest_traffic.CONFIG_URL: CONFIG_XML, ingest_traffic.MEASURED_URL: MEASURED_XML}
    written = []  # what would go into the sensor_readings INSERT
    before = ingest_traffic.ndw_bad_data_count

    with mock.patch.object(ingest_traffic, "download_and_decompress",
                           side_effect=lambda url: io.BytesIO(feeds[url])):
        code = ingest_traffic.main(["--no-upload", "--data-dir", str(tmp_path)],
                                   store=lambda rows: written.extend(rows) or len(rows))

    assert code == 0
    assert [(station, component, value) for station, _, component, value in written] == [
        ("NDW_hrl", "flow_1", 600.0)]  # the speed -1 row is skipped
    assert ingest_traffic.ndw_bad_data_count == before + 1
