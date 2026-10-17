"""
Kafka producer: replays July 2024 NYC taxi trips (never seen by the model) into topic `trips.live`
in event-time order, sped up so that one simulated hour passes every 3600/SPEEDUP real seconds.
This stands in for a live GPS/probe-vehicle feed.

  python3 replay_trips.py --speedup 360 --start "2024-07-01 06:00" --hours 48
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta

import pyarrow.compute as pc
import pyarrow.parquet as pq
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--file", default="/workspace/data/raw/yellow_tripdata_2024-07.parquet")
ap.add_argument("--start", default="2024-07-01 06:00")
ap.add_argument("--hours", type=int, default=48)
ap.add_argument("--speedup", type=float, default=360.0)
args = ap.parse_args()

start = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
end = start + timedelta(hours=args.hours)
t = pq.read_table(args.file, columns=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                                      "trip_distance", "PULocationID", "passenger_count"])
t = t.filter(pc.and_(pc.greater_equal(t["tpep_pickup_datetime"], start),
                     pc.less(t["tpep_pickup_datetime"], end)))
t = t.sort_by("tpep_pickup_datetime").to_pandas()
print(f"[replay] {len(t):,} trips between {start} and {end}, speed-up x{args.speedup:g}")

producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                         value_serializer=lambda v: json.dumps(v).encode(),
                         key_serializer=lambda k: str(k).encode(), linger_ms=50, batch_size=64 * 1024)

wall0, sent, next_report = time.time(), 0, start + timedelta(hours=1)
for i, r in enumerate(t.itertuples(index=False)):
    ev = r.tpep_pickup_datetime.to_pydatetime()
    lag = (ev - start).total_seconds() / args.speedup - (time.time() - wall0)
    if lag > 0:
        time.sleep(lag)
    producer.send("trips.live", key=int(r.PULocationID), value={
        "event_id": f"jul-{i}", "source": "nyc_tlc_replay", "zone_id": int(r.PULocationID),
        "pickup_ts": f"{ev:%Y-%m-%d %H:%M:%S}",
        "dropoff_ts": f"{r.tpep_dropoff_datetime.to_pydatetime():%Y-%m-%d %H:%M:%S}",
        "distance_mi": float(r.trip_distance),
        "passengers": None if r.passenger_count != r.passenger_count else int(r.passenger_count)})
    sent += 1
    if ev >= next_report:
        print(f"[replay] simulated {ev:%a %d %b %H:%M}  sent {sent:,} trips", flush=True)
        next_report += timedelta(hours=1)
producer.flush()
print(f"[replay] done, {sent:,} trips sent")
