"""Build training_data.csv from the readings the pipeline has stored in RDS (D-075).

Both sources come from sensor_readings, the data our own ingest services have
accumulated since they started on the VM:

- traffic: station_id NDW_<site>, component flow_<index> (one row per lane);
- NO2: station_id NL10240, component NO2, not flagged and not null.

It needs only pandas and psycopg2 (both in requirements.txt) and the PG*
settings of the database, the same ones the ingest services use. The database
accepts the VM and the laptop's /32, so it is run where those are set, e.g.
inside the air-ingest container:
    python build_training_data.py [output.csv]
Then, from airbreda/: python train.py  (writes model.pkl, D-076).
"""

import sys

import pandas as pd
import psycopg2

OUT = sys.argv[1] if len(sys.argv) > 1 else "training_data.csv"

conn = psycopg2.connect(sslmode="require")
traffic = pd.read_sql(
    """SELECT substr(station_id, 5) AS site_id, timestamp, value
       FROM sensor_readings
       WHERE station_id LIKE 'NDW\\_%' AND component LIKE 'flow\\_%' AND value >= 0""", conn)
no2 = pd.read_sql(
    """SELECT timestamp, value AS no2_ug_m3
       FROM sensor_readings
       WHERE station_id = 'NL10240' AND component = 'NO2'
         AND value IS NOT NULL AND NOT is_flagged""", conn)
conn.close()

# Traffic: one row per site and UTC hour, in the SAME unit the dashboard sends to
# predict(): the sum of the site's trafficFlow values (vehicles/hour, all lanes)
# in its one stored minute of that hour. The old version summed all four sites,
# which the dashboard never does (training-serving skew, ADR-006). The column
# keeps the name predict() expects. If a site has two minutes in an hour (a
# re-run), the later one is kept, as the dashboard reads the newest file.
per_minute = traffic.groupby(["site_id", "timestamp"], as_index=False)["value"].sum()
per_minute["hour_utc"] = pd.to_datetime(per_minute["timestamp"], utc=True).dt.floor("h")
per_hour = (per_minute.sort_values("timestamp").groupby(["site_id", "hour_utc"], as_index=False).last()
            .rename(columns={"value": "total_intensity_veh_per_hr"}))

# NO2: timestamp is the END of the hourly average (task 003, R2), so the average
# over 11:00-12:00 UTC is stored as 12:00. Subtract one hour to get the hour it
# covers, which is the hour the NDW minute falls in. One station serves all four
# sites, so each hour's NO2 value appears once per site.
no2["hour_utc"] = pd.to_datetime(no2["timestamp"], utc=True) - pd.Timedelta(hours=1)

df = per_hour.merge(no2[["hour_utc", "no2_ug_m3"]], on="hour_utc").sort_values(["hour_utc", "site_id"])
df["hour_of_day"] = df["hour_utc"].dt.hour
df[["total_intensity_veh_per_hr", "hour_of_day", "no2_ug_m3"]].to_csv(OUT, index=False)
print(df[["hour_utc", "site_id", "total_intensity_veh_per_hr", "hour_of_day", "no2_ug_m3"]].to_string(index=False))
print(f"{len(df)} row(s) from {df['hour_utc'].nunique()} hour(s) written to {OUT}")
