"""
Kafka producer: UrbanMind's existing Kolkata ingestion (Google News RSS feeds, and TomTom live
incidents if TOMTOM_API_KEY is set) published to topic `traffic.events` every few minutes.

This reuses the original app's fetchers unchanged (mounted read-only at /urbanmind-app). The
LLM extraction step of the main app is replaced here by a fast keyword tagger so the stream
needs no API key.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "/urbanmind-app")
from kafka import KafkaProducer  # noqa: E402

from ingestion.rss_fetcher import fetch_kolkata_city_feeds  # noqa: E402

TAGS = [("accident", r"accident|crash|collision|hit|overturn"),
        ("waterlogging", r"waterlog|flood|rain|inundat"),
        ("protest", r"protest|rally|march|strike|bandh|blockade|agitation"),
        ("closure", r"closed|closure|diversion|divert|restrict|shut"),
        ("construction", r"construction|repair|metro work|flyover|bridge work"),
        ("congestion", r"jam|congest|traffic snarl|gridlock|slow")]
SEVERITY = {"accident": 0.7, "waterlogging": 0.8, "protest": 0.6, "closure": 0.7,
            "construction": 0.5, "congestion": 0.5, "other": 0.2}
INTERVAL = int(os.getenv("EVENT_POLL_SECONDS", "120"))


def tag(text):
    for name, pattern in TAGS:
        if re.search(pattern, text, re.I):
            return name
    return "other"


def tomtom_events():
    if not os.getenv("TOMTOM_API_KEY"):
        return []
    try:
        from ingestion.tomtom_fetcher import fetch_tomtom_incidents
        return fetch_tomtom_incidents() or []
    except Exception as e:
        print(f"[events] TomTom skipped: {e}")
        return []


producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                         value_serializer=lambda v: json.dumps(v, default=str).encode())
seen = set()
while True:
    items = [("rss", a) for a in fetch_kolkata_city_feeds(max_items=8)] + [("tomtom", a) for a in tomtom_events()]
    new = 0
    for src, a in items:
        title = a.get("title") or a.get("description") or ""
        if not title or title in seen:
            continue
        seen.add(title)
        etype = tag(f"{title} {a.get('description', '')}")
        producer.send("traffic.events", {
            "city": "Kolkata", "source": f"{src}:{a.get('source', '')}", "title": title[:300],
            "event_type": etype, "severity": SEVERITY[etype],
            "published": a.get("published") or a.get("start_time") or "",
            "ingested_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "url": a.get("link") or a.get("source_url") or ""})
        new += 1
    producer.flush()
    print(f"[events] {datetime.now():%H:%M:%S} published {new} new Kolkata events (total {len(seen)})", flush=True)
    time.sleep(INTERVAL)
