#!/usr/bin/env python3
"""
MapReduce MAP phase (Hadoop Streaming).

Input : one taxi trip per line
        pickup_ts,dropoff_ts,passengers,distance_mi,pu_zone,do_zone,fare,total
Output: key   = pickup_zone \t date \t hour
        value = miles \t hours \t 1
Bad records (GPS glitches, zero/negative durations, impossible speeds) are dropped here,
and counted with Hadoop counters so they show up in the job summary.
"""
import sys
from datetime import datetime

FMT = "%Y-%m-%d %H:%M:%S"


def counter(name):
    sys.stderr.write(f"reporter:counter:UrbanMind,{name},1\n")


for line in sys.stdin:
    p = line.rstrip("\n").split(",")
    if len(p) < 6 or p[0] == "pickup_ts":
        continue
    try:
        start, end = datetime.strptime(p[0], FMT), datetime.strptime(p[1], FMT)
        miles, zone = float(p[3]), int(p[4])
    except ValueError:
        counter("MALFORMED")
        continue
    hours = (end - start).total_seconds() / 3600.0
    if not (start.year == 2024 and start.month <= 6 and 1 <= zone <= 263):
        counter("OUT_OF_RANGE")
        continue
    if not (0.1 <= miles <= 100 and 1 / 60 <= hours <= 3):
        counter("BAD_DISTANCE_OR_DURATION")
        continue
    if not (1 <= miles / hours <= 80):
        counter("IMPOSSIBLE_SPEED")
        continue
    counter("VALID_TRIPS")
    print(f"{zone}\t{start:%Y-%m-%d}\t{start.hour}\t{miles:.3f}\t{hours:.5f}\t1")
