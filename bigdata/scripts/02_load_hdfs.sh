#!/usr/bin/env bash
# Step 02: load the staged data into HDFS (the storage layer of the batch pipeline)
set -e
S=/workspace/data/staged
hdfs dfs -mkdir -p /urbanmind/raw/trips /urbanmind/ref/zones /urbanmind/ref/weather
hdfs dfs -put -f $S/trips/*.csv   /urbanmind/raw/trips/
hdfs dfs -put -f $S/zones/*.tsv   /urbanmind/ref/zones/
hdfs dfs -put -f $S/weather/*.tsv /urbanmind/ref/weather/

echo; echo "== HDFS contents"
hdfs dfs -ls -h /urbanmind/raw/trips
hdfs dfs -du -s -h /urbanmind/raw /urbanmind/ref
echo; echo "== block report for one file (128 MB blocks, replication 1)"
hdfs fsck /urbanmind/raw/trips/trips_2024-03.csv -files -blocks | grep -E "blocks|Total size|Total blocks" | head -5
