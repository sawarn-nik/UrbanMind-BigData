#!/usr/bin/env bash
# Step 07: speed layer. Kafka topics -> Spark Structured Streaming -> HDFS, plus the producers.
#   ./bd run 07            start everything in the background (logs in bigdata/output/logs/)
#   ./bd run 07 stop       stop the stream and producers
set -e
LOGS=/workspace/output/logs; mkdir -p $LOGS /workspace/output/live

if [ "$1" = "stop" ]; then
  pkill -f stream_congestion.py || true; pkill -f replay_trips.py || true; pkill -f urbanmind_events.py || true
  echo "stopped"; exit 0
fi

python3 - <<'EOF'
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
a = KafkaAdminClient(bootstrap_servers="kafka:9092")
for name, parts in [("trips.live", 3), ("traffic.events", 1)]:
    try:
        a.create_topics([NewTopic(name, num_partitions=parts, replication_factor=1)]); print("created topic", name)
    except TopicAlreadyExistsError:
        print("topic exists", name)
EOF

# fresh run: clear old stream output + checkpoints
hdfs dfs -rm -r -f -skipTrash /urbanmind/stream /urbanmind/checkpoints >/dev/null
rm -f /workspace/output/live/*.json

nohup spark-submit --master "local[3]" --driver-memory 1500m /workspace/spark/stream_congestion.py > $LOGS/stream.log 2>&1 &
echo "spark streaming starting (log: output/logs/stream.log) ..."
until grep -q "\[stream\] running" $LOGS/stream.log; do
  if ! pgrep -f stream_congestion.py >/dev/null; then echo "stream failed:"; tail -30 $LOGS/stream.log; exit 1; fi
  sleep 3
done

nohup python3 -u /workspace/producers/urbanmind_events.py > $LOGS/events_producer.log 2>&1 &
nohup python3 -u /workspace/producers/replay_trips.py --speedup ${SPEEDUP:-360} --hours ${HOURS:-48} > $LOGS/replay.log 2>&1 &
echo "producers started. Watch: tail -f bigdata/output/logs/{stream,replay,events_producer}.log"
echo "dashboard: ./bd run 08   then open http://localhost:8050"
