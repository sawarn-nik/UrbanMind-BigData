"""Generates docs/urbanmind_architecture.excalidraw. Open it at https://excalidraw.com (menu > Open)."""
import json
import os
import random

els = []
rid = lambda: "e%08x" % random.getrandbits(32)


def base(t, x, y, w, h, color, **kw):
    e = {"type": t, "id": rid(), "x": x, "y": y, "width": w, "height": h, "angle": 0, "strokeColor": color,
         "backgroundColor": "transparent", "fillStyle": "solid", "strokeWidth": 2, "strokeStyle": "solid",
         "roughness": 1, "opacity": 100, "groupIds": [], "frameId": None, "roundness": None,
         "seed": random.getrandbits(31), "version": 1, "versionNonce": random.getrandbits(31),
         "isDeleted": False, "boundElements": [], "updated": 1, "link": None, "locked": False}
    e.update(kw)
    els.append(e)
    return e


def text(x, y, s, color="#1e1e1e", size=16, align="left", container=None, w=None):
    lines = s.split("\n")
    w = w or max(len(l) for l in lines) * size * 0.6
    h = len(lines) * size * 1.25
    return base("text", x, y, w, h, color, text=s, originalText=s, fontSize=size, fontFamily=3,
                textAlign=align, verticalAlign="middle", containerId=container, lineHeight=1.25,
                autoResize=True, baseline=int(size))


def box(x, y, title, sub, color, w=210, h=90, shape="rectangle"):
    b = base(shape, x, y, w, h, color, roundness={"type": 3})
    t = text(x, y, f"{title}\n{sub}", color, 15, "center", container=b["id"], w=w - 10)
    t["x"], t["y"] = x + 5, y + (h - t["height"]) / 2
    b["boundElements"].append({"type": "text", "id": t["id"]})
    return b


def arrow(a, b, label=None, dashed=False, color="#1e1e1e"):
    ax, ay = a["x"] + a["width"], a["y"] + a["height"] / 2
    bx, by = b["x"], b["y"] + b["height"] / 2
    if abs(ay - by) > 60 and abs(ax - bx) < 300 and b["y"] > a["y"]:   # vertical
        ax, ay = a["x"] + a["width"] / 2, a["y"] + a["height"]
        bx, by = b["x"] + b["width"] / 2, b["y"]
    ar = base("arrow", ax, ay, bx - ax, by - ay, color, points=[[0, 0], [bx - ax, by - ay]],
              startArrowhead=None, endArrowhead="arrow", strokeStyle="dashed" if dashed else "solid",
              roundness={"type": 2}, startBinding={"elementId": a["id"], "focus": 0, "gap": 6},
              endBinding={"elementId": b["id"], "focus": 0, "gap": 6})
    a["boundElements"].append({"type": "arrow", "id": ar["id"]})
    b["boundElements"].append({"type": "arrow", "id": ar["id"]})
    if label:
        text((ax + bx) / 2 - len(label) * 4, (ay + by) / 2 - 22, label, "#868e96", 13)
    return ar


GRAY, BLUE, ORANGE, GREEN, YELLOW, PURPLE, RED = "#495057", "#1971c2", "#e8590c", "#2f9e44", "#f08c00", "#9c36b5", "#e03131"
X = [0, 300, 600, 900, 1200, 1500]

text(0, -170, "UrbanMind: Urban Traffic Congestion Prediction on Hadoop, Pig, Hive, Spark & Kafka", "#1e1e1e", 28)
text(0, -125, "Lambda architecture: a batch layer learns what traffic normally looks like; a speed layer scores what is happening now",
     "#868e96", 16)

# ── batch layer ──
text(0, -60, "BATCH LAYER  (historical, Jan-Jun 2024)", BLUE, 18)
src = box(X[0], 0, "Open data", "NYC TLC trips ~20M\nOpen-Meteo weather\ntaxi zone lookup", GRAY, h=110, shape="ellipse")
hdfs = box(X[1], 10, "HDFS", "/urbanmind/raw\n~1.3 GB CSV, 128 MB blocks", BLUE)
mr = box(X[2], 10, "MapReduce on YARN", "mapper / combiner / reducer\ntrip -> zone x hour speed", BLUE)
pig = box(X[3], 10, "Apache Pig", "clean, free-flow speed,\njoin zones + weather", PURPLE)
hive = box(X[4], 10, "Apache Hive", "partitioned by month\nbucketed by zone (ORC)", YELLOW)
ml = box(X[5], 10, "Spark MLlib", "GBT: next-hour speed\nvs baseline & LinReg", GREEN)
arrow(src, hdfs, "hdfs dfs -put"); arrow(hdfs, mr); arrow(mr, pig, "zone_hour"); arrow(pig, hive, "features")
arrow(hive, ml, "HiveQL / files")
text(X[2], 110, "key = zone, date, hour\nvalue = miles, hours, 1\nspeed = sum(miles)/sum(hours)", "#868e96", 12)
text(X[3], 110, "congestion index =\n1 - speed / free-flow\nLOW <0.25 <MOD <0.45 <HEAVY", "#868e96", 12)
text(X[4], 110, "8 analytics queries:\npeak hours, worst zones,\nrain impact, pruning, sampling", "#868e96", 12)
text(X[5], 110, "features: hour, weekday, zone,\nlag 1h/24h/168h, weather\ntrain Jan-May / test June", "#868e96", 12)

# ── speed layer ──
Y = 330
text(0, Y - 60, "SPEED LAYER  (live)", ORANGE, 18)
live = box(X[0], Y, "Live feeds", "NYC July trip replay\nKolkata RSS + TomTom\n(UrbanMind fetchers)", GRAY, h=110, shape="ellipse")
prod = box(X[1], Y + 10, "Python producers", "replay_trips.py\nurbanmind_events.py", GRAY)
kafka = box(X[2], Y + 10, "Kafka (KRaft)", "trips.live (3 partitions)\ntraffic.events", ORANGE)
ss = box(X[3], Y + 10, "Spark Structured\nStreaming", "1h window / 15 min slide\nwatermark 10 min", GREEN)
out = box(X[4], Y + 10, "Parquet on HDFS", "stream/zone_windows\nstream/events", BLUE)
dash = box(X[5], Y + 10, "Live dashboard", "localhost:8050\nwhat is flowing & stored", RED)
arrow(live, prod, "poll / replay"); arrow(prod, kafka, "JSON events"); arrow(kafka, ss, "subscribe")
arrow(ss, out, "foreachBatch"); arrow(out, dash, "watch")
text(X[2], Y + 110, '{ "zone_id": 161,\n  "pickup_ts": "2024-07-01 08:03",\n  "distance_mi": 1.9, ... }', "#868e96", 12)
text(X[3], Y + 110, "live speed per zone & window\n+ join serving profile\n-> congestion index, vs normal", "#868e96", 12)

# batch -> speed: serving profile + model
srv = arrow(ml, ss, None, dashed=True, color=GREEN)
text(X[4] + 40, 215, "serving profile (typical speed per\nzone / weekday / hour) + saved model", GREEN, 13)
arrow(hive, out, None, dashed=True, color=YELLOW)

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "urbanmind_architecture.excalidraw"), "w") as f:
    json.dump({"type": "excalidraw", "version": 2, "source": "https://excalidraw.com", "elements": els,
               "appState": {"viewBackgroundColor": "#ffffff", "gridSize": None}, "files": {}}, f, indent=1)
print(f"wrote {len(els)} elements")
