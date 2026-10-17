"""
UrbanMind: Spark Structured Streaming (speed layer).

  Kafka trips.live      -> parse JSON -> event-time windows (1 h window, sliding every 15 min,
                           10 min watermark for late trips) per zone -> live speed + trip count
                        -> join the batch layer's serving profile (typical speed for this zone /
                           weekday / hour, free-flow speed) -> live congestion index, anomaly vs normal
                        -> Parquet on HDFS (/urbanmind/stream/zone_windows) + dashboard snapshot
  Kafka traffic.events  -> Kolkata disruption events -> Parquet on HDFS (/urbanmind/stream/events)
"""
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

HDFS = "hdfs://hadoop:9000/urbanmind"
LIVE = "/workspace/output/live"
BOOT = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
os.makedirs(LIVE, exist_ok=True)

spark = (SparkSession.builder.appName("UrbanMind live congestion")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

profile = spark.read.parquet(f"{HDFS}/serving/zone_profile").cache()
print(f"[stream] serving profile loaded: {profile.count():,} zone/weekday/hour rows")

trip_schema = StructType([StructField("event_id", StringType()), StructField("source", StringType()),
                          StructField("zone_id", IntegerType()), StructField("pickup_ts", StringType()),
                          StructField("dropoff_ts", StringType()), StructField("distance_mi", DoubleType())])

trips = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", "trips.live").option("startingOffsets", "latest").load()
         .select(F.from_json(F.col("value").cast("string"), trip_schema).alias("t")).select("t.*")
         .withColumn("ts", F.to_timestamp("pickup_ts"))
         .withColumn("hours", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 3600.0)
         .filter((F.col("distance_mi").between(0.1, 100)) & (F.col("hours").between(1 / 60, 3)))
         .filter((F.col("distance_mi") / F.col("hours")).between(1, 80)))

windows = (trips.withWatermark("ts", "10 minutes")
           .groupBy(F.window("ts", "1 hour", "15 minutes"), "zone_id")
           .agg(F.count("*").alias("trips"), F.sum("distance_mi").alias("miles"), F.sum("hours").alias("hours")))

state = {"batches": 0, "rows_written": 0, "latest": {}}


def score_windows(batch, batch_id):
    if batch.rdd.isEmpty():
        return
    scored = (batch
              .withColumn("window_start", F.date_format("window.start", "yyyy-MM-dd HH:mm"))
              .withColumn("window_end", F.date_format("window.end", "yyyy-MM-dd HH:mm"))
              .withColumn("dow", F.dayofweek("window.start"))           # 1 = Sunday in Spark
              .withColumn("dow", F.when(F.col("dow") == 1, 7).otherwise(F.col("dow") - 1))  # 1 = Monday, as in Pig
              .withColumn("hr", F.hour("window.start"))
              .withColumn("live_speed", F.round(F.col("miles") / F.col("hours"), 3))
              .drop("window", "miles", "hours")
              .join(profile, ["zone_id", "dow", "hr"], "inner")   # only zones the batch layer has a profile for
              .withColumn("congestion_index", F.round(F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                          1 - F.col("live_speed") / F.col("freeflow"))), 4))
              .withColumn("vs_normal", F.round(F.col("live_speed") / F.col("hist_speed"), 3))
              .withColumn("level", F.when(F.col("congestion_index") < 0.25, "LOW")
                          .when(F.col("congestion_index") < 0.45, "MODERATE").otherwise("HEAVY"))
              .withColumn("batch_id", F.lit(batch_id)))
    scored.write.mode("append").parquet(f"{HDFS}/stream/zone_windows")

    rows = scored.filter(F.col("trips") >= 5).collect()
    state["batches"] += 1
    state["rows_written"] += len(rows)
    for r in rows:
        state["latest"][(r["zone_id"], r["window_start"])] = r.asDict()
    # keep the newest window that has substantial data
    starts = sorted({k[1] for k in state["latest"]})
    for k in [k for k in state["latest"] if k[1] < (starts[-6] if len(starts) > 6 else "")]:
        del state["latest"][k]
    counts = {s: sum(1 for k in state["latest"] if k[1] == s) for s in starts[-6:]}
    current = max(counts, key=lambda s: (counts[s] >= 0.6 * max(counts.values()), s))
    zones = sorted([v for k, v in state["latest"].items() if k[1] == current],
                   key=lambda v: -(v["congestion_index"] or 0))
    snap = {"batch_id": batch_id, "batches": state["batches"], "window_start": current,
            "zones": [{k: v[k] for k in ("zone_id", "zone", "borough", "trips", "live_speed", "hist_speed",
                                          "freeflow", "congestion_index", "vs_normal", "level")} for v in zones]}
    with open(f"{LIVE}/latest.tmp", "w") as f:
        json.dump(snap, f, default=str)
    os.replace(f"{LIVE}/latest.tmp", f"{LIVE}/latest.json")
    print(f"[stream] batch {batch_id}: window {current}  {len(zones)} zones scored", flush=True)


event_schema = StructType([StructField(n, StringType()) for n in
                           ("city", "source", "title", "event_type", "published", "ingested_ts", "url")]
                          + [StructField("severity", DoubleType())])
events = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
          .option("subscribe", "traffic.events").option("startingOffsets", "earliest").load()
          .select(F.from_json(F.col("value").cast("string"), event_schema).alias("e")).select("e.*"))


def save_events(batch, batch_id):
    if batch.rdd.isEmpty():
        return
    batch.write.mode("append").parquet(f"{HDFS}/stream/events")
    path = f"{LIVE}/events.json"
    old = json.load(open(path)) if os.path.exists(path) else []
    new = [r.asDict() for r in batch.collect()]
    json.dump((new + old)[:100], open(path, "w"), default=str)


q1 = (windows.writeStream.outputMode("update").foreachBatch(score_windows)
      .option("checkpointLocation", f"{HDFS}/checkpoints/zone_windows").trigger(processingTime="5 seconds")
      .queryName("zone_windows").start())
q2 = (events.writeStream.foreachBatch(save_events)
      .option("checkpointLocation", f"{HDFS}/checkpoints/events").trigger(processingTime="10 seconds")
      .queryName("kolkata_events").start())
print("[stream] running: trips.live -> zone windows, traffic.events -> events", flush=True)
spark.streams.awaitAnyTermination()
