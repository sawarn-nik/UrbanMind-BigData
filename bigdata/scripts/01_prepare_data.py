"""
Step 01: turn the downloaded open data into plain-text files for Hadoop/Pig/Hive.

  data/raw/yellow_tripdata_2024-01..06.parquet  ->  data/staged/trips/trips_2024-MM.csv   (batch layer)
  data/raw/yellow_tripdata_2024-07.parquet      ->  kept as parquet, replayed into Kafka   (speed layer)
  data/raw/taxi_zone_lookup.csv                 ->  data/staged/zones/zones.tsv
  data/raw/weather_nyc_2024.json (Open-Meteo)   ->  data/staged/weather/weather.tsv
"""
import csv
import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

BASE = "/workspace/data"
RAW, STAGED = f"{BASE}/raw", f"{BASE}/staged"
BATCH_MONTHS = ["01", "02", "03", "04", "05", "06"]
COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
HEADER = ["pickup_ts", "dropoff_ts", "passengers", "distance_mi",
          "pu_zone", "do_zone", "fare", "total"]

for d in ("trips", "zones", "weather"):
    os.makedirs(f"{STAGED}/{d}", exist_ok=True)

total = 0
for m in BATCH_MONTHS:
    out = f"{STAGED}/trips/trips_2024-{m}.csv"
    t = pq.read_table(f"{RAW}/yellow_tripdata_2024-{m}.parquet", columns=COLS)
    for i in (0, 1):  # whole seconds, otherwise strftime's %S prints "55.000000"
        ts = pc.cast(t.column(i), pa.timestamp("s"), safe=False)
        t = t.set_column(i, HEADER[i], pc.strftime(ts, format="%Y-%m-%d %H:%M:%S"))
    t = t.rename_columns(HEADER)
    with open(out, "wb") as f:
        f.write((",".join(HEADER) + "\n").encode())
        pacsv.write_csv(t, f, pacsv.WriteOptions(include_header=False, quoting_style="none"))
    total += t.num_rows
    print(f"  trips 2024-{m}: {t.num_rows:>10,} rows -> {out} ({os.path.getsize(out)/1e6:,.0f} MB)")
print(f"  batch layer total: {total:,} trips")

with open(f"{RAW}/taxi_zone_lookup.csv") as f, open(f"{STAGED}/zones/zones.tsv", "w") as out:
    rows = list(csv.reader(f))[1:]
    for r in rows:
        out.write("\t".join(r) + "\n")
print(f"  zones: {len(rows)} rows")

w = json.load(open(f"{RAW}/weather_nyc_2024.json"))["hourly"]
with open(f"{STAGED}/weather/weather.tsv", "w") as out:
    for i, ts in enumerate(w["time"]):
        dt, hh = ts.split("T")
        out.write("\t".join(map(str, [dt, int(hh[:2]), w["temperature_2m"][i], w["precipitation"][i],
                                      w["snowfall"][i], w["wind_speed_10m"][i]])) + "\n")
print(f"  weather: {len(w['time'])} hourly rows")
