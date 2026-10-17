#!/usr/bin/env bash
# Starts a pseudo-distributed Hadoop node (HDFS + YARN + JobHistory) and keeps the container alive.
set -e
mkdir -p /data/hdfs /data/tmp /data/metastore /workspace/output /var/log/urbanmind

if [ ! -d /data/hdfs/nn/current ]; then
  echo "[entrypoint] formatting HDFS NameNode (first start)"
  hdfs namenode -format -force -nonInteractive urbanmind
fi

hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver

hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /tmp/hive /user/hive/warehouse /urbanmind
hdfs dfs -chmod -R 777 /tmp

if [ ! -d /data/metastore/metastore_db ]; then
  echo "[entrypoint] initialising Hive metastore (Derby)"
  (cd /data/metastore && schematool -dbType derby -initSchema > /var/log/urbanmind/schematool.log 2>&1) \
    && echo "[entrypoint] metastore ready" || echo "[entrypoint] schematool failed, see /var/log/urbanmind/schematool.log"
fi

echo "[entrypoint] UrbanMind big-data node is up:"
echo "  HDFS  http://localhost:9870   YARN http://localhost:8088   JobHistory http://localhost:19888"
tail -f /dev/null
