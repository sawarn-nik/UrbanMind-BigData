#!/usr/bin/env bash
# Step 06: Spark MLlib model (PySpark), Scala report job, and Hive tables over the predictions
set -e
mkdir -p /workspace/output/hive

echo "== Spark MLlib training"
PYTHONUNBUFFERED=1 spark-submit --master "local[4]" --driver-memory 2g /workspace/spark/train_model.py 2>&1 \
  | grep --line-buffered -vE "WARN|INFO|^\s*$" | tee /workspace/output/spark_train.log

echo; echo "== Scala Spark job"
cd /workspace/spark && spark-shell --master "local[2]" --driver-memory 1g -i ZoneReport.scala 2>&1 \
  | grep -vE "WARN|INFO|^scala>|^\s*$|^Welcome|^Using|^Type|^Spark context|^Spark session|^      " | tee /workspace/output/scala_report.log

echo; echo "== Hive over predictions"
cd /data/metastore && hive -S -f /workspace/hive/03_predictions.hql 2>/workspace/output/hive/03_predictions.err \
  | tee /workspace/output/hive/03_predictions.txt
