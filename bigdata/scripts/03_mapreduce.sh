#!/usr/bin/env bash
# Step 03: Hadoop MapReduce on YARN (Hadoop Streaming, Python mapper/combiner/reducer)
#   raw trips (HDFS)  ->  zone x hour aggregates: trips, miles, average speed, average duration
set -e
OUT=/urbanmind/mr/zone_hour
MR=/workspace/mapreduce
hdfs dfs -rm -r -f -skipTrash $OUT

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -D mapreduce.job.name="UrbanMind zone-hour congestion aggregation" \
  -D stream.num.map.output.key.fields=3 \
  -D stream.num.reduce.output.key.fields=3 \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -D mapreduce.job.reduces=4 \
  -files $MR/mapper.py,$MR/reducer.py \
  -mapper  "python3 mapper.py" \
  -combiner "python3 reducer.py combine" \
  -reducer "python3 reducer.py" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input  /urbanmind/raw/trips \
  -output $OUT

echo; echo "== output files (one per reducer)"; hdfs dfs -ls -h $OUT
echo "== zone-hour rows: $(hdfs dfs -cat $OUT/part-* | wc -l)   duplicate keys: $(hdfs dfs -cat $OUT/part-* | cut -f1-3 | sort | uniq -d | wc -l)"
echo "== sample (zone, date, hour, trips, miles, avg_speed_mph, avg_duration_min)"
hdfs dfs -cat $OUT/part-00000 | head -8
