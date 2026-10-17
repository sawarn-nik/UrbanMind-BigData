"""
UrbanMind pipeline dashboard: a small HTTP server that exposes the state of every layer as JSON
(Kafka offsets, HDFS sizes, live congestion windows, Kolkata events, model metrics) and serves
index.html. Runs inside the big-data container on port 8050.
"""
import json
import os
import subprocess
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from kafka import KafkaConsumer, TopicPartition

OUT = "/workspace/output"
HERE = os.path.dirname(os.path.abspath(__file__))
HDFS_DIRS = ["raw/trips", "mr/zone_hour", "features/zone_hour", "predictions/june_2024",
             "models/gbt_speed", "stream/zone_windows", "stream/events"]
cache = {"hdfs": {}, "kafka": {}, "hdfs_ts": 0}


def load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default


def hdfs_poll():
    while True:
        sizes = {}
        try:
            out = subprocess.run(["hdfs", "dfs", "-count"] + [f"/urbanmind/{d}" for d in HDFS_DIRS],
                                 capture_output=True, text=True, timeout=60).stdout
            for line in out.splitlines():
                p = line.split()
                if len(p) == 4:
                    sizes[p[3].replace("hdfs://hadoop:9000", "").replace("/urbanmind/", "")] = {
                        "files": int(p[1]), "bytes": int(p[2])}
        except Exception as e:
            sizes["error"] = str(e)
        cache["hdfs"], cache["hdfs_ts"] = sizes, time.time()
        time.sleep(15)


def kafka_poll():
    consumer = None
    while True:
        try:
            consumer = consumer or KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
            stats = {}
            for topic in ("trips.live", "traffic.events"):
                parts = consumer.partitions_for_topic(topic) or set()
                tps = [TopicPartition(topic, p) for p in parts]
                stats[topic] = {"partitions": len(tps),
                                "messages": sum(consumer.end_offsets(tps).values()) if tps else 0}
            prev = cache["kafka"]
            for t, s in stats.items():
                old = prev.get(t, {})
                dt = time.time() - old.get("_ts", time.time())
                s["rate"] = round((s["messages"] - old.get("messages", s["messages"])) / dt, 1) if dt > 0 else 0
                s["_ts"] = time.time()
            cache["kafka"] = stats
        except Exception as e:
            cache["kafka"] = {"error": str(e)}
            consumer = None
        time.sleep(2)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=HERE, **kw)

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path.startswith("/api/state"):
            body = json.dumps({
                "kafka": cache["kafka"], "hdfs": cache["hdfs"],
                "live": load(f"{OUT}/live/latest.json", {}),
                "events": load(f"{OUT}/live/events.json", [])[:30],
                "metrics": load(f"{OUT}/metrics.json", {}),
                "now": time.time()}, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            super().do_GET()


threading.Thread(target=hdfs_poll, daemon=True).start()
threading.Thread(target=kafka_poll, daemon=True).start()
print("[dashboard] http://localhost:8050", flush=True)
ThreadingHTTPServer(("0.0.0.0", 8050), Handler).serve_forever()
