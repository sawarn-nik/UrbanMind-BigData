"""
UrbanMind: Spark MLlib congestion model (batch layer).

Task      : predict next-hour average traffic speed of every taxi zone, then turn it into a
            congestion index (1 - speed / free-flow speed) and a level LOW / MODERATE / HEAVY.
Data      : Pig feature table on HDFS (Jan-Jun 2024 zone-hours)
Split     : by time. Train Jan-May, test June (no shuffling, so no future leakage)
Features  : time (hour, day of week, weekend), zone (id, free-flow, historical profile),
            recent history (speed 1h / 24h / 168h ago, trips 1h ago), weather
Models    : historical-average baseline vs Linear Regression vs Gradient-Boosted Trees
Outputs   : model + predictions (Parquet) + serving profile on HDFS, metrics.json for the dashboard
"""
import json

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

HDFS = "hdfs://hadoop:9000/urbanmind"
OUT = "/workspace/output"

spark = (SparkSession.builder.appName("UrbanMind congestion model")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "16")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField(n, t) for n, t in [
    ("zone_id", IntegerType()), ("borough", StringType()), ("zone", StringType()), ("dt", StringType()),
    ("hr", IntegerType()), ("dow", IntegerType()), ("is_weekend", IntegerType()), ("trips", LongType()),
    ("avg_speed", DoubleType()), ("avg_dur", DoubleType()), ("temp_c", DoubleType()),
    ("precip_mm", DoubleType()), ("snow_cm", DoubleType()), ("wind_kmh", DoubleType()),
    ("freeflow", DoubleType()), ("congestion_index", DoubleType()), ("level", StringType())]])

df = spark.read.csv(f"{HDFS}/features/zone_hour", sep="\t", schema=schema)
df = df.withColumn("hidx", (F.unix_timestamp(
    F.concat_ws(" ", "dt", F.lpad(F.col("hr").cast("string"), 2, "0")), "yyyy-MM-dd HH") / 3600).cast("long"))
df.cache()
print(f"[spark] feature rows: {df.count():,}  zones: {df.select('zone_id').distinct().count()}")

# ── lag features: same zone, 1 hour / 1 day / 1 week earlier (self-joins on an hour index) ──
def lagged(hours, name, col="avg_speed"):
    return df.select("zone_id", (F.col("hidx") + hours).alias("hidx"), F.col(col).alias(name))

for h, name in [(1, "lag1"), (24, "lag24"), (168, "lag168")]:
    df = df.join(lagged(h, name), ["zone_id", "hidx"], "left")
df = df.join(lagged(1, "trips_lag1", "trips"), ["zone_id", "hidx"], "left")

is_train = F.col("dt") < "2024-06-01"

# ── historical profile per zone x day-of-week x hour, learned from the training months only ──
profile = (df.filter(is_train).groupBy("zone_id", "dow", "hr")
           .agg(F.avg("avg_speed").alias("hist_speed"), F.avg("trips").alias("hist_trips")))
zone_mean = df.filter(is_train).groupBy("zone_id").agg(F.avg("avg_speed").alias("zone_mean"))

df = (df.join(profile, ["zone_id", "dow", "hr"], "left").join(zone_mean, "zone_id", "left")
      .withColumn("hist_speed", F.coalesce("hist_speed", "zone_mean", F.col("freeflow") * 0.75))
      .withColumn("hist_trips", F.coalesce("hist_trips", F.lit(0.0)))
      .withColumn("lag1", F.coalesce("lag1", "hist_speed"))
      .withColumn("lag24", F.coalesce("lag24", "hist_speed"))
      .withColumn("lag168", F.coalesce("lag168", "hist_speed"))
      .withColumn("trips_lag1", F.coalesce(F.col("trips_lag1").cast("double"), "hist_trips")))

FEATURES = ["hr", "dow", "is_weekend", "zone_id", "freeflow", "hist_speed", "hist_trips",
            "lag1", "lag24", "lag168", "trips_lag1", "temp_c", "precip_mm", "snow_cm", "wind_kmh"]
# materialise the joined feature table once, otherwise every training iteration re-runs the joins
df.write.mode("overwrite").parquet(f"{HDFS}/features/model_input")
df = spark.read.parquet(f"{HDFS}/features/model_input").cache()
train, test = df.filter(is_train).cache(), df.filter(~is_train).cache()
n_train, n_test = train.count(), test.count()
print(f"[spark] train rows (Jan-May): {n_train:,}   test rows (June): {n_test:,}")

assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
gbt = GBTRegressor(labelCol="avg_speed", predictionCol="pred_gbt", maxIter=60, maxDepth=6,
                   stepSize=0.1, subsamplingRate=0.8, maxBins=64, seed=42)
lr = LinearRegression(labelCol="avg_speed", predictionCol="pred_lr", regParam=0.01, solver="normal")

print("[spark] training Linear Regression ...")
lr_model = Pipeline(stages=[assembler, lr]).fit(train)
print("[spark] training Gradient-Boosted Trees (60 trees, depth 6) ...")
gbt_model = Pipeline(stages=[assembler, gbt]).fit(train)

pred = gbt_model.transform(lr_model.transform(test).drop("features"))

# ── congestion index + level from predicted speed ──
def ci(speed_col):
    return F.greatest(F.lit(0.0), F.least(F.lit(1.0), 1 - F.col(speed_col) / F.col("freeflow")))

def level_code(ci_col):
    return F.when(ci_col < 0.25, 0.0).when(ci_col < 0.45, 1.0).otherwise(2.0)

pred = (pred.withColumn("actual_ci", ci("avg_speed")).withColumn("pred_ci", ci("pred_gbt"))
        .withColumn("base_ci", ci("hist_speed"))
        .withColumn("actual_lvl", level_code(F.col("actual_ci")))
        .withColumn("pred_lvl", level_code(F.col("pred_ci")))
        .withColumn("base_lvl", level_code(F.col("base_ci"))))
pred.cache()

metrics = {"train_rows": n_train, "test_rows": n_test, "features": FEATURES, "models": {}, "classification": {}}
for name, col in [("baseline_hist_avg", "hist_speed"), ("linear_regression", "pred_lr"), ("gbt", "pred_gbt")]:
    m = {k: round(RegressionEvaluator(labelCol="avg_speed", predictionCol=col, metricName=k).evaluate(pred), 4)
         for k in ("rmse", "mae", "r2")}
    metrics["models"][name] = m
    print(f"[spark] {name:<20} RMSE {m['rmse']:.3f} mph   MAE {m['mae']:.3f} mph   R2 {m['r2']:.3f}")

for name, col in [("baseline_hist_avg", "base_lvl"), ("gbt", "pred_lvl")]:
    ev = MulticlassClassificationEvaluator(labelCol="actual_lvl", predictionCol=col)
    m = {k: round(ev.setMetricName(k).evaluate(pred), 4) for k in ("accuracy", "f1")}
    metrics["classification"][name] = m
    print(f"[spark] level {name:<14} accuracy {m['accuracy']:.3f}   F1 {m['f1']:.3f}")

imp = gbt_model.stages[-1].featureImportances
metrics["feature_importance"] = sorted(
    [{"feature": f, "importance": round(float(imp[i]), 4)} for i, f in enumerate(FEATURES)],
    key=lambda x: -x["importance"])
metrics["level_distribution_test"] = {
    {0.0: "LOW", 1.0: "MODERATE", 2.0: "HEAVY"}[r["actual_lvl"]]: r["count"]
    for r in pred.groupBy("actual_lvl").count().collect()}

# ── outputs ──
gbt_model.write().overwrite().save(f"{HDFS}/models/gbt_speed")

lvl = lambda c: F.when(F.col(c) == 0, "LOW").when(F.col(c) == 1, "MODERATE").otherwise("HEAVY")
(pred.select("zone_id", "borough", "zone", "dt", "hr", "dow", "trips",
             F.round("avg_speed", 3).alias("actual_speed"), F.round("pred_gbt", 3).alias("pred_gbt"),
             F.round("pred_lr", 3).alias("pred_lr"), F.round("hist_speed", 3).alias("baseline_speed"),
             "freeflow", F.round("actual_ci", 4).alias("actual_ci"), F.round("pred_ci", 4).alias("pred_ci"),
             lvl("actual_lvl").alias("actual_level"), lvl("pred_lvl").alias("pred_level"))
 .coalesce(4).write.mode("overwrite").parquet(f"{HDFS}/predictions/june_2024"))

zone_info = df.select("zone_id", "borough", "zone", "freeflow").distinct()
serving = (profile.join(zone_info, "zone_id")
           .select("zone_id", "borough", "zone", "dow", "hr", F.round("hist_speed", 3).alias("hist_speed"),
                   F.round("hist_trips", 1).alias("hist_trips"), "freeflow"))
serving.coalesce(1).write.mode("overwrite").parquet(f"{HDFS}/serving/zone_profile")

# one busy zone, one week of June: actual vs predicted, for the dashboard chart
busiest = (test.groupBy("zone_id", "zone").agg(F.sum("trips").alias("t")).orderBy(F.desc("t")).first())
week = (pred.filter((F.col("zone_id") == busiest["zone_id"]) & (F.col("dt").between("2024-06-10", "2024-06-16")))
        .orderBy("dt", "hr").select("dt", "hr", "avg_speed", "pred_gbt", "hist_speed").collect())
metrics["forecast_sample"] = {
    "zone": busiest["zone"],
    "points": [{"t": f"{r['dt']} {r['hr']:02d}:00", "actual": round(r["avg_speed"], 2),
                "gbt": round(r["pred_gbt"], 2), "baseline": round(r["hist_speed"], 2)} for r in week]}

with open(f"{OUT}/metrics.json", "w") as f:
    json.dump(metrics, f, indent=2)
print(f"[spark] model -> {HDFS}/models/gbt_speed, predictions -> {HDFS}/predictions/june_2024, metrics -> output/metrics.json")
spark.stop()
