# UrbanMind: Video Script (Group 30, ~11 minutes)

**Topic:** Urban Traffic Congestion Prediction · **Team:** Nikhil Kumar, Gaurav Kumar, Abhay Devanand Sawale
**Format:** screen recording. Excalidraw diagram for the walkthrough, terminal + browser for the demo.
Speak to the screen, point with the cursor at the box you are talking about (like the reference video).

**Before recording**
- `./bigdata/bd up` (cluster running), batch steps 01-06 already run once (outputs exist)
- Tabs open: excalidraw.com with `docs/urbanmind_architecture.excalidraw`, HDFS UI `localhost:9870`,
  YARN UI `localhost:8088`, JobHistory `localhost:19888`, dashboard `localhost:8050`
- Terminal font size large, `cd ~/UrbanMind`

---

## 1. Problem & idea (≈1 min) · speaker: Nikhil · screen: UrbanMind map (images/user-interface.png) then diagram title

> "Traffic congestion costs cities hours every day, and most tools only react *after* a jam forms.
> Our project, UrbanMind, started as a Kolkata trip planner that reads news, police advisories and
> TomTom incidents to warn about disruptions. For this course we asked: how do we *predict*
> congestion city-wide, from millions of trips, and also score what is happening *right now*?
> That is a big-data problem: the volume of trip records is too large for a single Python script,
> and the live feed never stops. So we rebuilt the data side of UrbanMind on Hadoop, Pig, Hive,
> Spark and Kafka."

> "Kolkata has no open historical traffic data, so for the batch layer we use the New York City
> taxi trip records, which are public, about 20 million trips for January to June 2024. The pipeline is
> city-agnostic; Kolkata comes in through the live layer."

## 2. Architecture walkthrough (≈3 min) · speaker: Nikhil · screen: Excalidraw diagram

Walk left to right, top row then bottom row.

> "We follow the **Lambda architecture**: a batch layer that learns what traffic normally looks
> like from history, and a speed layer that scores live data against it."

**Batch layer**
1. *Open data → HDFS*: "Raw trips, weather from Open-Meteo, and the zone lookup are put into HDFS:
   1.3 GB of CSV, split into 128 MB blocks."
2. *MapReduce on YARN*: "The mapper reads each trip, drops bad records (zero distance, impossible speeds),
   and emits the key *(zone, date, hour)* with *(miles, hours, 1)*. A combiner pre-sums on the map side,
   the reducer outputs trip count and average speed = total miles / total hours per zone-hour."
3. *Pig*: "Pig cleans and enriches: it computes each zone's free-flow speed from night hours,
   joins zone names and hourly weather with replicated joins, and computes the
   **congestion index = 1 − speed / free-flow speed**, labelled LOW, MODERATE or HEAVY."
4. *Hive*: "Hive gives us SQL over HDFS. External tables over each stage, and a managed ORC table
   **partitioned by month** and **bucketed by zone**, for partition pruning and sampling."
5. *Spark MLlib*: "Spark trains a Gradient-Boosted Trees model to predict next-hour speed per zone,
   with time, zone profile, lagged speeds 1 hour / 1 day / 1 week ago, and weather. We train on
   January-May and test on June, compared against a historical-average baseline."

**Speed layer**
6. *Producers → Kafka*: "Two producers: one replays July trips (data the model never saw) in
   event-time order as a live feed; the other reuses UrbanMind's own Kolkata RSS/TomTom fetchers.
   Messages are JSON, keyed by zone, on topics trips.live and traffic.events."
7. *Spark Structured Streaming*: "It reads Kafka, groups trips into **1-hour event-time windows sliding
   every 15 minutes**, with a **10-minute watermark** so late trips are still counted. Each micro-batch joins
   the batch layer's serving profile to get the congestion index and how far traffic is from normal."
8. *HDFS + dashboard*: "Results land as Parquet on HDFS and on a live dashboard."

## 3. What we built on (≈1 min) · speaker: Gaurav · screen: diagram, zoom on boxes

| Idea | Source | Where we use it |
|---|---|---|
| MapReduce | Dean & Ghemawat, OSDI 2004 | zone-hour aggregation |
| Pig Latin | Olston et al., SIGMOD 2008 | ETL data flow |
| Hive | Thusoo et al., VLDB 2009 | warehouse + HiveQL |
| Kafka | Kreps et al., NetDB 2011 | event log for live feeds |
| Lambda architecture | Marz & Warren, *Big Data*, 2015 | batch + speed layers |
| Structured Streaming | Armbrust et al., SIGMOD 2018 | windows + watermarks |
| Traffic forecasting (DCRNN) | Li et al., ICLR 2018 | next-hour speed prediction task |

## 4. Batch demo (≈3 min) · speakers: Gaurav (HDFS, MapReduce, Pig), Abhay (Hive, Spark)

Run each command live, or show the output already captured in `bigdata/output/`.

1. **HDFS**: `./bigdata/bd run 02` output, or the browser at `localhost:9870` → Utilities → Browse → `/urbanmind/raw/trips`.
   Point at 6 files, 1.3 GB, block count.
2. **MapReduce**: show `mapreduce/mapper.py` briefly, then the job in YARN UI `localhost:8088`
   (the finished "UrbanMind zone-hour congestion aggregation" job) and the counters
   (VALID_TRIPS, IMPOSSIBLE_SPEED …) from `output/03_mapreduce.log`. Show sample output rows.
3. **Pig**: show `pig/congestion_features.pig` (point at FILTER, GROUP, JOIN … USING 'replicated', the
   congestion index line), then the sample feature rows and busiest zones from the step 04 output.
4. **Hive**: `./bigdata/bd shell` → `cd /data/metastore && hive` →
   `USE urbanmind; SHOW PARTITIONS congestion;` then run Q3 (most congested zones) and Q5 (rain impact) live,
   or show `output/hive/02_analytics.txt`.
5. **Spark MLlib**: show `output/spark_train.log` (RMSE of baseline vs Linear Regression vs GBT) and the
   **MODEL** tab of the dashboard (metrics, feature importance, forecast-vs-actual chart).
   Show the Scala job output `output/scala_report.log`.

## 5. Live demo (≈2 min) · speaker: Nikhil · screen: terminal + dashboard

1. `./bigdata/bd run 07` → topics created, Spark streaming started, producers started.
2. Dashboard `localhost:8050` **PIPELINE** tab: Kafka message counter climbing, msg/s, Spark batch id,
   HDFS stream folder growing.
3. **LIVE CONGESTION** tab: event-time window advancing (simulated July hours), zones sorted by
   congestion index, "vs normal" column showing where traffic is slower than usual.
4. **KOLKATA EVENTS** tab: real Kolkata headlines flowing through Kafka from UrbanMind's fetchers.
5. Optionally `tail -f bigdata/output/logs/stream.log` to show micro-batches.
6. `./bigdata/bd run 07 stop`.

## 6. Results, limitations, future work (≈1 min) · speaker: Abhay

- **Numbers to quote** (from `bigdata/output/`):
  - 20.33M raw trips → 19.79M valid after the mapper's checks (459K bad distance/duration, 77K out of range, 8K impossible speeds)
  - Combiner cut shuffle from 19.8M records to 639K (31×); 481,723 zone-hours; Pig keeps 237,878 with ≥ 5 trips
  - Hive: weekday city speed falls from 23.1 mph at 4 AM to ~9.7 mph at 3-5 PM; worst evening-peak zone Midtown Center
    (8.4 mph, congestion index 0.62); rain lowers Manhattan daytime speed 10.9 → 10.2 mph
  - Model, June hold-out (41,780 zone-hours): next-hour speed RMSE baseline 2.26, Linear Regression 2.18, GBT 2.20 mph (R² ≈ 0.80);
    congestion level accuracy baseline 73.7% → GBT 76.0%; Queens RMSE 3.49 → 3.05 with GBT
  - Most important feature: the historical zone/weekday/hour profile (0.74), then the speed 1 hour ago
  - Be honest: GBT and Linear Regression are close; the gain over the historical average is modest because city traffic is
    highly regular, and the model helps most where traffic is less predictable (Queens, airports) and in classifying the level.
- Limitations: taxi speed is a *proxy* for road speed; NYC model does not transfer directly to Kolkata;
  single-node cluster (pseudo-distributed) rather than a real multi-node cluster.
- Future: Kolkata probe/GPS data, model applied inside the stream with lag state, HBase/Cassandra as a
  serving store, GraphFrames over the road network, UrbanMind routing using predicted congestion as edge cost.

> "To sum up: 20 million trips go through MapReduce, Pig, Hive and Spark MLlib to learn congestion patterns,
> and Kafka plus Structured Streaming score live traffic against them. Thank you."
