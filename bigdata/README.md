# UrbanMind: A Big Data Approach (pipeline details)

CSE412 Big Data & Large-Scale Computing, Group 30 (Nikhil Kumar, Gaurav Kumar, Abhay Devanand Sawale)

The data layer of UrbanMind rebuilt on **Hadoop (HDFS, YARN, MapReduce), Pig, Hive, Spark (MLlib, Structured
Streaming, Scala) and Kafka**, in a Lambda architecture:

- **Batch layer**: 20.3M NYC taxi trips (Jan-Jun 2024) + hourly weather → HDFS → MapReduce → Pig → Hive → Spark MLlib
  model that predicts next-hour traffic speed and congestion level for every zone.
- **Speed layer**: July trips replayed as a live feed + UrbanMind's Kolkata RSS/TomTom fetchers → Kafka →
  Spark Structured Streaming (event-time windows, watermark) → Parquet on HDFS → live dashboard.

Architecture diagram: `docs/urbanmind_architecture.excalidraw` (open at excalidraw.com). Video script: `docs/VIDEO_SCRIPT.md`.

## Run it

Needs Docker Desktop (5 GB memory). Data downloads (~400 MB):

```bash
mkdir -p bigdata/data/raw && cd bigdata/data/raw
for m in 01 02 03 04 05 06 07; do curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet; done
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
curl -o weather_nyc_2024.json "https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.006&start_date=2024-01-01&end_date=2024-07-31&hourly=temperature_2m,precipitation,rain,snowfall,wind_speed_10m&timezone=America%2FNew_York"
cd ../..
```

```bash
./bd up            # build + start Hadoop/Hive/Pig/Spark node and Kafka
./bd run 01        # parquet -> CSV/TSV
./bd run 02        # load into HDFS
./bd run 03        # MapReduce on YARN (~3 min)
./bd run 04        # Pig ETL (~3 min)
./bd run 05        # Hive tables + analytics (~8 min)
./bd run 06        # Spark MLlib + Scala job + Hive over predictions (~5 min)
./bd run 08        # dashboard  -> http://localhost:8050
./bd run 07        # live layer: Kafka + Structured Streaming + producers   (./bd run 07 stop)
./bd down          # stop (HDFS data kept)   |   ./bd reset  (wipe)
```

Web UIs: HDFS `localhost:9870`, YARN `localhost:8088`, JobHistory `localhost:19888`, Spark `localhost:4040` (while running).
All step outputs are saved in `output/`.

## Layout

| Path | Tool | Purpose |
|---|---|---|
| `docker/` | Hadoop 3.3.6, Pig 0.17, Hive 3.1.3, Spark 3.5.3, Java 8 | single-node cluster image + configs |
| `mapreduce/` | Hadoop Streaming | mapper / combiner / reducer: trips → zone × hour speed |
| `pig/` | Pig Latin | free-flow speed, replicated joins with zones & weather, congestion index |
| `hive/` | HiveQL | external tables, partitioned + bucketed ORC table, analytics, predictions table |
| `spark/train_model.py` | Spark MLlib | GBT vs Linear Regression vs historical baseline |
| `spark/ZoneReport.scala` | Spark (Scala) | per-borough error + top congested zones |
| `spark/stream_congestion.py` | Structured Streaming | Kafka → windows → congestion index → HDFS |
| `producers/` | Kafka | July trip replay, UrbanMind Kolkata events |
| `dashboard/` | Python + HTML | live view of every layer |

## Results (June 2024 hold-out)

| Model | RMSE (mph) | MAE (mph) | R² | Level accuracy |
|---|---|---|---|---|
| Historical average (zone × weekday × hour) | 2.263 | 1.568 | 0.784 | 73.7% |
| Linear Regression | 2.181 | 1.466 | 0.799 | – |
| Gradient-Boosted Trees | 2.196 | 1.475 | 0.796 | 76.0% |

Congestion index = 1 − speed / free-flow speed (free-flow = the zone's night-time speed). LOW < 0.25 ≤ MODERATE < 0.45 ≤ HEAVY.

**Limitations**: taxi trip speed is a proxy for road speed; the model is trained on NYC (no open historical
Kolkata data exists); the cluster is pseudo-distributed on one machine.
