#!/usr/bin/env python3
"""
MapReduce COMBINE / REDUCE phase (Hadoop Streaming).

Keys arrive sorted, so all values for one (zone, date, hour) are adjacent.
  reducer.py combine -> partial sums (same format as the mapper), runs map-side to cut shuffle traffic
  reducer.py         -> final row per zone-hour:
                        zone \t date \t hour \t trips \t miles \t avg_speed_mph \t avg_duration_min
Average speed is total miles / total hours (distance-weighted), not a mean of per-trip speeds.
"""
import sys

combine = len(sys.argv) > 1 and sys.argv[1] == "combine"
cur, miles, hours, trips = None, 0.0, 0.0, 0


def emit():
    if cur is None:
        return
    if combine:
        print(f"{cur}\t{miles:.3f}\t{hours:.5f}\t{trips}")
    else:
        print(f"{cur}\t{trips}\t{miles:.2f}\t{miles / hours:.3f}\t{hours * 60 / trips:.2f}")


for line in sys.stdin:
    p = line.rstrip("\n").split("\t")
    key = "\t".join(p[:3])
    if key != cur:
        emit()
        cur, miles, hours, trips = key, 0.0, 0.0, 0
    miles += float(p[3])
    hours += float(p[4])
    trips += int(p[5])
emit()
