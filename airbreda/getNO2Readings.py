"""
Fetch the latest NO2 reading for RIVM Luchtmeetnet station NL10240
(Breda-Tilburgseweg) using the official open API.

API docs: https://api-docs.luchtmeetnet.nl/
No API key required. Fair use limit: 100 requests / 5 minutes.
"""

import requests

STATION = "NL10240"
FORMULA = "NO2"

BASE_URL = f"https://api.luchtmeetnet.nl/open_api/stations/{STATION}/measurements"


def get_no2_measurements(station: str = STATION, formula: str = FORMULA) -> list:
    """Return the newest page of measurements (newest first) as a list of dicts.

    One HTTP request, no retries here: retrying is the caller's decision.
    """
    params = {
        "formula": formula,
        "order_by": "timestamp_measured",
        "order_direction": "desc",
        "page": 1,
    }
    url = f"https://api.luchtmeetnet.nl/open_api/stations/{station}/measurements"

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Validate the shape before using it, so an unexpected body raises a clear
    # ValueError instead of an AttributeError/TypeError deep in the caller.
    if not isinstance(data, dict):
        raise ValueError(f"expected a JSON object, got {type(data).__name__}")
    measurements = data.get("data", [])
    if not isinstance(measurements, list):
        raise ValueError(f"expected 'data' to be a list, got {type(measurements).__name__}")
    if not measurements:
        raise ValueError(f"No measurements found for station {station} ({formula})")
    for record in measurements:
        if not isinstance(record, dict):
            raise ValueError(
                f"expected each measurement to be an object, got {type(record).__name__}"
            )

    return measurements


def get_latest_no2(station: str = STATION, formula: str = FORMULA) -> dict:
    """Return the most recent measurement for the given station/formula."""
    return get_no2_measurements(station, formula)[0]


if __name__ == "__main__":
    latest = get_latest_no2()
    print(f"Station:    {STATION}")
    print(f"Component:  {latest['formula']}")
    print(f"Value:      {latest['value']} µg/m³")
    print(f"Timestamp:  {latest['timestamp_measured']}")