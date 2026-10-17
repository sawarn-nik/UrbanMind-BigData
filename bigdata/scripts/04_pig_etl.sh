#!/usr/bin/env bash
# Step 04: Apache Pig ETL on MapReduce. Joins zone-hour aggregates with zones + weather into features.
set -e
OUT=/urbanmind/features/zone_hour
TOP=/urbanmind/features/top_zones
hdfs dfs -rm -r -f -skipTrash $OUT $TOP

PARAMS="-param MR=/urbanmind/mr/zone_hour -param ZONES=/urbanmind/ref/zones -param WEATHER=/urbanmind/ref/weather -param OUT=$OUT -param TOP=$TOP"

if ! pig -x mapreduce $PARAMS /workspace/pig/congestion_features.pig 2>&1 | tee /workspace/output/pig.log | grep -E "Success|Failed|ERROR|Input\(s\)|Output\(s\)|records"; then :; fi
if ! hdfs dfs -test -e $OUT/_SUCCESS; then
  echo "[pig] mapreduce mode did not finish, retrying in local mode against HDFS"
  hdfs dfs -rm -r -f -skipTrash $OUT $TOP
  pig -x local -param MR=hdfs://hadoop:9000/urbanmind/mr/zone_hour -param ZONES=hdfs://hadoop:9000/urbanmind/ref/zones \
      -param WEATHER=hdfs://hadoop:9000/urbanmind/ref/weather -param OUT=hdfs://hadoop:9000$OUT -param TOP=hdfs://hadoop:9000$TOP \
      /workspace/pig/congestion_features.pig 2>&1 | tee /workspace/output/pig.log | grep -E "Success|Failed|ERROR" || true
fi

echo; echo "== feature rows: $(hdfs dfs -cat $OUT/part-* | wc -l)"
echo "== sample features (zone_id borough zone dt hr dow weekend trips speed dur temp precip snow wind freeflow ci level)"
hdfs dfs -cat $OUT/part-* | head -5
echo; echo "== busiest zones"; hdfs dfs -cat $TOP/part-*
