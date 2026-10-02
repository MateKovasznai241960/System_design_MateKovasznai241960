"""
Fetch current traffic flow/speed for the A27 sites near NL10240 (Breda-Tilburgseweg
air quality station) directly from NDW's open data portal.

Downloads:
  - snelheden_en_intensiteiten_configuratie_meetlocaties.xml.gz  (site config)
  - snelheden_en_intensiteiten_meetgegevens.xml.gz                (live measured values)
"""

import gzip
import io
import urllib.request
import xml.etree.ElementTree as ET

CONFIG_URL = "http://opendata.ndw.nu/snelheden_en_intensiteiten_configuratie_meetlocaties.xml.gz"
MEASURED_URL = "http://opendata.ndw.nu/snelheden_en_intensiteiten_meetgegevens.xml.gz"

# A27 sites closest to NL10240 (Breda-Tilburgseweg), road "027" = A27, hectometer ~63.
# Two mainline directions + the entry/exit ramps at that interchange.
TARGET_SITE_IDS = [
    "RWS01_MONIBAS_0271hrl0063ra",  # A27 mainline, direction 1
    "RWS01_MONIBAS_0271hrr0063ra",  # A27 mainline, direction 2
    "RWS01_MONIBAS_0270vwd0063ra",  # entry slip road
    "RWS01_MONIBAS_0270vwa0063ra",  # exit slip road
]


def local_tag(tag):
    return tag.split('}')[-1] if '}' in tag else tag


def download_and_decompress(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=180) as resp:
        compressed = resp.read()
    return io.BytesIO(gzip.decompress(compressed))


def build_index_map(config_source, site_id):
    """config_source: file path (str) or a file-like object (e.g. from download_and_decompress)."""
    context = ET.iterparse(config_source, events=("start", "end"))
    _, root = next(context)

    index_map = {}
    in_target_site = False
    current_index = None
    current_type = None
    current_vehicle = None

    for event, elem in context:
        tag = local_tag(elem.tag)

        if event == "start" and tag == "measurementSite":
            in_target_site = (elem.attrib.get("id") == site_id)

        if in_target_site and event == "start" and tag == "measurementSpecificCharacteristics" \
                and "index" in elem.attrib:
            current_index = elem.attrib["index"]
            current_type = None
            current_vehicle = None

        if in_target_site and event == "end" and tag == "specificMeasurementValueType":
            current_type = (elem.text or "").strip()

        if in_target_site and event == "end" and tag == "vehicleType":
            current_vehicle = (elem.text or "").strip()  # "anyVehicle" for totals

        if in_target_site and event == "end" and tag == "measurementSpecificCharacteristics" \
                and "index" in elem.attrib:
            if current_type:
                index_map[current_index] = {"type": current_type, "vehicle": current_vehicle or "specific-class"}
            current_index = None

        if event == "end" and tag == "measurementSite":
            in_target_site = False
            root.clear()

    return index_map


def extract_measurements(measured_source, site_id):
    """measured_source: file path (str) or a file-like object."""
    context = ET.iterparse(measured_source, events=("start", "end"))
    _, root = next(context)

    results = []
    in_target = False
    current_timestamp = None
    current_index = None
    current_type = None
    current_value = None

    for event, elem in context:
        tag = local_tag(elem.tag)

        if event == "start" and tag == "siteMeasurements":
            in_target = False
            current_timestamp = None

        if event == "start" and tag == "measurementSiteReference":
            if elem.attrib.get("id") == site_id:
                in_target = True

        if in_target and event == "start" and tag == "physicalQuantity" and "index" in elem.attrib:
            current_index = elem.attrib["index"]

        if in_target and event == "start" and tag == "basicData":
            for k, v in elem.attrib.items():
                if local_tag(k) == "type":
                    current_type = v.split(":")[-1]

        if in_target and event == "end" and tag == "vehicleFlowRate":
            current_value = (elem.text or "").strip()

        if in_target and event == "end" and tag == "speed" and current_type == "TrafficSpeed":
            current_value = (elem.text or "").strip()

        if in_target and event == "end" and tag == "timeValue":
            current_timestamp = (elem.text or "").strip()

        if in_target and event == "end" and tag == "physicalQuantity" and "index" in elem.attrib \
                and current_index is not None and current_value is not None:
            results.append({"index": current_index, "type": current_type, "value": current_value})
            current_index = None
            current_type = None
            current_value = None

        if event == "end" and tag == "siteMeasurements":
            if in_target:
                for r in results:
                    r.setdefault("timestamp", current_timestamp)
            in_target = False
            root.clear()

    return results


def report_site(config_bytes, measured_bytes, site_id):
    """config_bytes / measured_bytes: raw decompressed bytes, reused across sites via io.BytesIO."""
    index_map = build_index_map(io.BytesIO(config_bytes), site_id)
    flow_idx = sorted((i for i, info in index_map.items()
                        if info["type"] == "trafficFlow" and info["vehicle"] == "anyVehicle"), key=int)
    speed_idx = sorted((i for i, info in index_map.items()
                         if info["type"] == "trafficSpeed" and info["vehicle"] == "anyVehicle"), key=int)

    readings = extract_measurements(io.BytesIO(measured_bytes), site_id)
    flow_vals = [float(r["value"]) for r in readings if r["index"] in flow_idx]
    speed_vals = [float(r["value"]) for r in readings if r["index"] in speed_idx and float(r["value"]) > 0]
    ts = readings[0].get("timestamp") if readings else None

    avg_speed = sum(speed_vals) / len(speed_vals) if speed_vals else None
    print(f"Site: {site_id}")
    print(f"  lanes            : {len(flow_idx)}")
    print(f"  flow per lane    : {flow_vals}")
    print(f"  TOTAL flow       : {sum(flow_vals):.0f} vehicles/hour")
    if avg_speed is not None:
        print(f"  avg speed        : {avg_speed:.1f} km/h")
    print(f"  timestamp        : {ts}")
    print()

    return {"site_id": site_id, "total_flow": sum(flow_vals), "avg_speed": avg_speed, "timestamp": ts}


if __name__ == "__main__":
    print("Downloading config file...")
    config_bytes = download_and_decompress(CONFIG_URL).read()

    print("Downloading measured-data file...")
    measured_bytes = download_and_decompress(MEASURED_URL).read()

    print(f"\nReading {len(TARGET_SITE_IDS)} A27 sites near NL10240 (Breda)...\n")
    summaries = []
    for site_id in TARGET_SITE_IDS:
        summaries.append(report_site(config_bytes, measured_bytes, site_id))

    mainline_total = sum(s["total_flow"] for s in summaries
                          if "hrl" in s["site_id"] or "hrr" in s["site_id"])
    entering_breda = next((s for s in summaries if "vwa" in s["site_id"]), None)  # exitSlipRoad -> into Breda
    leaving_breda = next((s for s in summaries if "vwd" in s["site_id"]), None)   # entrySlipRoad -> out of Breda

    print("=== A27 MAINLINE TOTAL (both directions) ===")
    print(f"{mainline_total:.0f} vehicles/hour\n")

    print("=== BREDA RAMP TRAFFIC (this interchange only) ===")
    if entering_breda:
        print(f"Entering Breda (A27 exit ramp) : {entering_breda['total_flow']:.0f} vehicles/hour, "
              f"avg speed {entering_breda['avg_speed']:.1f} km/h" if entering_breda['avg_speed'] is not None
              else f"Entering Breda (A27 exit ramp) : {entering_breda['total_flow']:.0f} vehicles/hour")
    if leaving_breda:
        print(f"Leaving Breda (A27 entry ramp) : {leaving_breda['total_flow']:.0f} vehicles/hour, "
              f"avg speed {leaving_breda['avg_speed']:.1f} km/h" if leaving_breda['avg_speed'] is not None
              else f"Leaving Breda (A27 entry ramp) : {leaving_breda['total_flow']:.0f} vehicles/hour")
