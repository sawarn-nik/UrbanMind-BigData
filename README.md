# UrbanMind: A Big Data Approach

**Urban traffic congestion prediction with Hadoop, Pig, Hive, Spark and Kafka.**

CSE412 Big Data & Large-Scale Computing · Group 30

Cities lose hours every day to traffic congestion, and most tools only react after a jam has formed.
UrbanMind predicts congestion instead: it learns what traffic normally looks like from **20 million taxi trips**,
predicts next-hour speed and congestion level for every zone of the city, and scores **live traffic** against
that baseline as it streams in.

The volume (1.3 GB of raw trip records, 20.3M rows) and the never-ending live feed make this a big data
problem, so the whole data pipeline runs on the Hadoop ecosystem in a **Lambda architecture**.

---

## Architecture

```
                ┌──────────────────────── BATCH LAYER (historical, Jan–Jun 2024) ────────────────────────┐
 NYC TLC trips  │                                                                                        │
 (20.3M rows) ──┼─► HDFS ──► MapReduce on YARN ──► Pig ETL ──────► Hive warehouse ──► Spark MLlib ───────┤
 Open-Meteo     │   raw      trip → zone×hour      free-flow speed,  partitioned by     GBT: next-hour   │
 weather,       │   1.3 GB   speed + trip count   zone & weather   month, bucketed    speed + level      │
 zone lookup    │                                 joins, congestion by zone (ORC),                       │
                │                                 index            8 analytics queries                   │
                └──────────────────────────────────────────────────────────────────────┬─────────────────┘
                                                                     serving profile   │ (typical speed per zone/weekday/hour)
                                                                                       │     
                ┌──────────────────────── SPEED LAYER (live) ──────────────────────────▼───────────────────┐
 July trips     │                                                                                          │
 (replayed) ────┼─► Python producers ─► Kafka ─► Spark Structured Streaming ─► Parquet on HDFS ─► Dashboard│
 Kolkata RSS /  │                        trips.live   1 h windows, 15 min slide,                           │
 TomTom feeds ──┤                        traffic.events  10 min watermark,                                 │
                │                                     live congestion index vs normal                      │
                └──────────────────────────────────────────────────────────────────────────────────────────┘
```

An editable diagram is in [`bigdata/docs/urbanmind_architecture.excalidraw`](bigdata/docs/urbanmind_architecture.excalidraw)
(open it at [excalidraw.com](https://excalidraw.com)).

### How each course module is used

| Module | Tool | What it does in UrbanMind |
|---|---|---|
| 1 | **HDFS** | Stores 6 months of raw trips (1.3 GB CSV, 128 MB blocks), reference data, features, models and stream output |
| 1 | **MapReduce + YARN** | Python mapper/combiner/reducer (Hadoop Streaming): validates 20.3M trips and aggregates them into speed and trip count per zone per hour |
| 2 | **Apache Pig** | ETL: free-flow speed per zone, replicated joins with zone names and hourly weather, congestion index and level |
| 3 | **Apache Hive** | External tables over every stage, a month-partitioned and zone-bucketed ORC table, views and 8 analytics queries |
| 4 | **Spark MLlib** | Predicts next-hour speed: Gradient-Boosted Trees vs Linear Regression vs a historical-average baseline |
| 4 | **Scala** | Spark job reporting per-borough model error and the most congested zones |
| 4 | **Kafka + Spark Structured Streaming** | Live trips and Kolkata disruption events through Kafka; event-time windows with watermarks score live congestion |

### Congestion index

```
congestion index = 1 − (current speed / free-flow speed)        free-flow = the zone's average night-time (1–5 AM) speed
LOW < 0.25 ≤ MODERATE < 0.45 ≤ HEAVY
```

---

## Data

| Dataset | Source | Size | Used for |
|---|---|---|---|
| NYC Yellow Taxi trips, Jan–Jun 2024 | [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | 20.3M trips | batch layer |
| NYC Yellow Taxi trips, July 2024 | NYC TLC | ~3M trips | replayed as the live stream (never seen by the model) |
| Taxi zone lookup | NYC TLC | 265 zones | zone names and boroughs |
| Hourly weather (temperature, rain, snow, wind) | [Open-Meteo](https://open-meteo.com) archive API | 5,112 hours | model features, rain-impact analysis |
| Kolkata traffic news, police advisories, TomTom incidents | UrbanMind's ingestion modules | live | Kolkata disruption stream |

All data is free and needs no sign-up. Kolkata has no open historical traffic data, so the batch layer is trained
and validated on NYC; the pipeline itself is city-agnostic, and Kolkata enters through the live layer.

---

## Results

### Pipeline

| Stage | Output |
|---|---|
| MapReduce | 20,332,099 records read → 19,787,309 valid trips (459K bad distance/duration, 77K out of range, 8K impossible speeds dropped) |
| | Combiner cut the shuffle from 19.8M to 639K records (31×) → **481,723 zone-hours** |
| Pig | **237,878** feature rows (zone-hours with at least 5 trips) |
| Hive | 6 month partitions × 8 zone buckets |
| Streaming | ~460 messages/s through Kafka, ~40 zones scored every 5 s |

### What the data shows (HiveQL)

- Weekday city-wide speed falls from **23.1 mph at 4 AM to about 9.7 mph at 3–5 PM**.
- Most congested zones in the weekday evening peak: **Midtown Center** (8.4 mph, index 0.62), Times Square, Midtown East.
- **Rain** lowers Manhattan daytime speed from 10.9 to 10.2 mph (congestion index 0.40 → 0.43).
- Manhattan carries 17.7M of the trips and is HEAVY in 30% of zone-hours.

### Model (train Jan–May, test on June 2024: 41,780 zone-hours)

| Model | RMSE (mph) | MAE (mph) | R² | Level accuracy |
|---|---|---|---|---|
| Historical average (zone × weekday × hour) | 2.263 | 1.568 | 0.784 | 73.7% |
| Linear Regression | 2.181 | 1.466 | 0.799 | – |
| Gradient-Boosted Trees | 2.196 | 1.475 | 0.796 | **76.0%** |

The strongest feature is the zone's historical weekday/hour profile, followed by the speed one hour earlier.
City traffic is very regular, so the gain over the historical average is modest. The model helps most where traffic
is less predictable (Queens RMSE 3.49 → 3.05 mph) and in classifying the congestion level. For the ten most
congested evening-peak zones, the predicted congestion index is within about 0.01 of the actual value.

Saved outputs from every step are in [`bigdata/output/`](bigdata/output/).

---

## Run it

**Requirements:** Docker Desktop with 5 GB memory and 4 CPUs (tested on an 8 GB Apple Silicon Mac).

```bash
git clone https://github.com/sawarn-nik/UrbanMind-BigData.git
cd UrbanMind-BigData/bigdata

# download the data (~400 MB)
mkdir -p data/raw && cd data/raw
for m in 01 02 03 04 05 06 07; do curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet; done
curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
curl -o weather_nyc_2024.json "https://archive-api.open-meteo.com/v1/archive?latitude=40.7128&longitude=-74.006&start_date=2024-01-01&end_date=2024-07-31&hourly=temperature_2m,precipitation,rain,snowfall,wind_speed_10m&timezone=America%2FNew_York"
cd ../..

./bd up          # build and start the cluster (first build ~15 min)
./bd run 01      # parquet → CSV/TSV
./bd run 02      # load into HDFS
./bd run 03      # MapReduce on YARN          (~3 min)
./bd run 04      # Pig ETL                    (~3 min)
./bd run 05      # Hive tables + analytics    (~8 min)
./bd run 06      # Spark MLlib + Scala + Hive (~5 min)
./bd run 08      # dashboard → http://localhost:8050
./bd run 07      # live layer: Kafka + Structured Streaming + producers   (stop: ./bd run 07 stop)
./bd down        # stop (HDFS data is kept)  ·  ./bd reset  to wipe everything
```

| Web UI | URL |
|---|---|
| Pipeline dashboard | http://localhost:8050 |
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| MapReduce JobHistory | http://localhost:19888 |
| Spark (while a job runs) | http://localhost:4040 |

Optional: set `TOMTOM_API_KEY` before `./bd up` to add live TomTom incidents to the Kolkata event stream.

### Stack

Single-node (pseudo-distributed) cluster in Docker: Hadoop 3.3.6 (HDFS, YARN, MapReduce), Pig 0.17.0,
Hive 3.1.3 (Derby metastore), Spark 3.5.3 (PySpark, MLlib, Structured Streaming, Scala shell), Kafka 3.8.0 (KRaft), Java 8.

---

## Repository layout

```
UrbanMind-BigData/
├── bigdata/                     ← the big data pipeline
│   ├── bd                       one-command helper (up / run <step> / down)
│   ├── docker-compose.yml       Hadoop/Hive/Pig/Spark node + Kafka
│   ├── docker/                  image + Hadoop/YARN/Hive configs
│   ├── scripts/                 pipeline steps 01–08
│   ├── mapreduce/               mapper.py, reducer.py (combiner + reducer)
│   ├── pig/                     congestion_features.pig
│   ├── hive/                    tables, analytics, predictions (HiveQL)
│   ├── spark/                   train_model.py (MLlib), ZoneReport.scala, stream_congestion.py
│   ├── producers/               Kafka producers: July trip replay, Kolkata events
│   ├── dashboard/               live pipeline dashboard
│   ├── docs/                    architecture diagram, video script
│   └── output/                  saved results and logs of every step
└── app/                         UrbanMind application (Kolkata trip planner)
```

---

## Application layer: the UrbanMind app

The pipeline is built on top of [UrbanMind](https://github.com/sawarn-nik/UrbanMind), a Kolkata multimodal trip planner
(FastAPI + Leaflet map) that reads news, police advisories and TomTom incidents to anticipate disruptions on a route.
Its ingestion modules (`app/ingestion/`) are reused unchanged as the Kafka producer for the Kolkata event stream.

To run the app itself:

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r req.txt
cp .env.example .env            # add an OpenRouter or Gemini key; TomTom/NewsAPI/OpenWeather optional
cd app && uvicorn api:app --port 8000
```

---

## Limitations and future work

- Taxi trip speed is a **proxy** for road speed, and only zones with enough trips are covered.
- The model is trained on NYC; Kolkata needs its own historical probe/GPS data to train a local model.
- The cluster is pseudo-distributed on one machine, not a multi-node cluster.
- Next steps: apply the model inside the stream with lag state, store live results in HBase/Cassandra for serving,
  graph analytics on the road network (GraphFrames), and feed predicted congestion into UrbanMind's route scoring.

---

## Contributors

| Name | Contributions |
|---|---|
| **Nikhil Kumar** ([@sawarn-nik](https://github.com/sawarn-nik)) | Architecture, Kafka producers, Spark Structured Streaming, UrbanMind integration, dashboard |
| **Gaurav Kumar** ([@Gaurav200604](https://github.com/Gaurav200604))| HDFS data loading, MapReduce job, Pig ETL |
| **Abhay Devanand Sawale** | Hive warehouse and analytics, Spark MLlib model and evaluation |

---

## License

MIT
