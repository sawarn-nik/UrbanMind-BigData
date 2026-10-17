#!/usr/bin/env bash
# Step 05: Apache Hive. Builds the warehouse tables over HDFS, then runs the analytics queries.
set -e
cd /data/metastore   # Derby metastore lives here
export HADOOP_CLIENT_OPTS="-Xmx768m"
mkdir -p /workspace/output/hive

echo "== building tables (partitioned + bucketed ORC load runs as a MapReduce job)"
hive -S -f /workspace/hive/01_tables.hql 2>/workspace/output/hive/01_tables.err | tee /workspace/output/hive/01_tables.txt

echo; echo "== analytics"
hive -S -f /workspace/hive/02_analytics.hql 2>/workspace/output/hive/02_analytics.err | tee /workspace/output/hive/02_analytics.txt
echo; echo "results saved to bigdata/output/hive/"
