#!/usr/bin/env python3

"""
Seismic Event (Session) Windows

Reads the "seismic-backfill" topic (7 days, from seismic-backfill.py) and
groups earthquakes into clusters of activity per location, using SESSION
windows on ORIGIN time:

  - Location key: the EMSC Flynn region (e.g. "CENTRAL ITALY"). See location().
  - A cluster (session) for a location stays open as long as each new quake
    there occurs within GAP_MIN of the previous one. A quiet gap longer than
    GAP_MIN ends the cluster; the next quake starts a new one.

Unlike the 1-hour tumbling windows in seismic-1h-tumbles.py, session windows
have no fixed size or alignment: their boundaries come from the data, and
each location has its own independent windows.

Two phases:
  1. History: replay the whole topic from the beginning, then print every
     cluster that is already closed.
  2. Live:    keep consuming; once a second, close and print any cluster whose
     last quake is more than GAP_MIN older than (now - GRACE_MIN).

Clusters with fewer than MIN_EVENTS quakes are closed silently (most regions
see isolated single events). Each event appears in Kafka several times
(create, updates, backfill), so open events are stored by unid: duplicates
collapse to one event and the most recent magnitude wins.
"""

import os
import json
from datetime import datetime, timezone
from collections import defaultdict
from confluent_kafka import TopicPartition, OFFSET_BEGINNING
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
# NOTE: HISTORY_HOURS must cover the topic's time span, or older events are silently dropped.
# For the live "seismic" topic (24h retention), use 24.
TOPIC = "seismic-backfill"
GAP_SEC = 60 * 60       # inactivity gap (origin time) that ends a cluster
GRACE_MIN = 30          # how long to wait for late reports before closing a cluster
MIN_EVENTS = 3          # smallest cluster worth printing
HISTORY_HOURS = 24 * 7  # ignore events older than this (matches the 7-day backfill)

# Kafka app; offsets are never committed, so every run replays the full history
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="seismic-event-windows",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4", "enable.auto.commit": False},
)

open_events = {}   # unid -> (origin_ts, location, mag) for events in not-yet-closed clusters
emitted = {}       # unid -> origin_ts for events already reported (ignore their later updates)
watermark = None   # clusters ending before this are closed; None until history is replayed


# The key that defines "a particular location". Swap in a lat/lon grid cell,
# e.g. f"{round(lat)},{round(lon)}", for fixed-size areas instead of named regions.
def location(p):
    return p["flynn_region"]


# Group open events by location, sort by origin time, and split wherever the
# gap between consecutive quakes exceeds GAP_SEC. Yields (location, [(ts, unid, mag), ...]).
def build_sessions():
    by_loc = defaultdict(list)
    for unid, (ts, loc, mag) in open_events.items():
        by_loc[loc].append((ts, unid, mag))
    for loc, evs in by_loc.items():
        evs.sort()
        session = [evs[0]]
        for e in evs[1:]:
            if e[0] - session[-1][0] > GAP_SEC:
                yield loc, session
                session = [e]
            else:
                session.append(e)
        yield loc, session


# Print one cluster as a single row
def print_session(loc, evs):
    fmt = lambda ts: datetime.fromtimestamp(ts, timezone.utc).strftime("%m-%d %H:%M")
    start, end = evs[0][0], evs[-1][0]
    dur = int(end - start) // 60
    mags = [m for _, _, m in evs if m is not None]
    max_mag = f"{max(mags):3.1f}" if mags else " - "
    print(f"{fmt(start)} -> {fmt(end)} UTC | {dur // 60:3d}h{dur % 60:02d}m "
          f"| events={len(evs):3d} | max_mag={max_mag} | {loc}")


# Close every cluster whose last quake + GAP_SEC is at or before `wm`; print the
# ones with at least MIN_EVENTS (oldest first) and move their events to `emitted`
def emit_closed(wm):
    global watermark
    watermark = wm
    closed = [(loc, evs) for loc, evs in build_sessions() if evs[-1][0] + GAP_SEC <= wm]
    for loc, evs in sorted(closed, key=lambda s: s[1][0][0]):
        if len(evs) >= MIN_EVENTS:
            print_session(loc, evs)
        for ts, unid, _ in evs:
            del open_events[unid]
            emitted[unid] = ts
    # Forget reported events once they age out of the history horizon
    cutoff = datetime.now(timezone.utc).timestamp() - HISTORY_HOURS * 3600
    for unid in [u for u, ts in emitted.items() if ts < cutoff]:
        del emitted[unid]


# Add one Kafka message to the open events; return False if it was skipped
def handle(raw):
    try:
        p = json.loads(raw)["data"]["properties"]
        ts = datetime.fromisoformat(p["time"].replace("Z", "+00:00")).timestamp()
        unid, loc = p["unid"], location(p)
    except (KeyError, ValueError, TypeError) as e:
        print(f"  skipping malformed message: {e}")
        return False

    if ts < datetime.now(timezone.utc).timestamp() - HISTORY_HOURS * 3600 or unid in emitted:
        return False
    # Late = its cluster would already be closed, and it doesn't touch any still-open quake there
    if (watermark is not None and unid not in open_events and ts + GAP_SEC <= watermark
            and not any(l == loc and abs(t - ts) <= GAP_SEC for t, l, _ in open_events.values())):
        print(f"  LATE, dropped: {unid} M{p.get('mag')} origin {p['time']} ({loc})")
        return False
    open_events[unid] = (ts, loc, p.get("mag"))
    return True


# Poll once; return the message value, or None if the topic is idle
def poll_value(consumer):
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        return None
    if msg.error():
        print(f"Kafka error: {msg.error()}")
        return b""
    return msg.value()


# Replay history, print closed clusters, then stream live clusters until Ctrl-C
def run():
    now = lambda: datetime.now(timezone.utc).timestamp()
    try:
        with app.get_consumer() as consumer:
            # Assign every partition at offset 0 directly (no group rebalance, no commits)
            parts = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
            consumer.assign([TopicPartition(TOPIC, p, OFFSET_BEGINNING) for p in parts])
            print(f"Replaying '{TOPIC}' ({len(parts)} partitions) on {KAFKA_BROKER}\n")

            # Phase 1: read until a 1 s poll comes back empty (history exhausted)
            msgs = 0
            while (value := poll_value(consumer)) is not None:
                if value:
                    msgs += handle(value)
            print(f"History: {msgs} messages -> {len(open_events)} distinct events")
            print(f"Clusters: >= {MIN_EVENTS} quakes per location, split by > {GAP_SEC // 60} min gaps\n")
            emit_closed(now() - GRACE_MIN * 60)

            # Phase 2: keep consuming; close clusters by wall clock minus grace
            print(f"\n--- live: each cluster prints {GAP_SEC // 60 + GRACE_MIN} min after its last quake ---\n")
            while True:
                if value := poll_value(consumer):
                    handle(value)
                emit_closed(now() - GRACE_MIN * 60)
    except KeyboardInterrupt:
        print("Stopped")
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")


if __name__ == "__main__":
    run()
