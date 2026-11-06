#!/usr/bin/env python3

"""
Seismic 1-Hour Tumbling Windows

Reads the "seismic-backfill" topic (7 days, from seismic-backfill.py) and
reports, for each 1-hour window of earthquake
ORIGIN time, the number of distinct events and their average magnitude.

Two phases:
  1. History: replay the whole topic from the beginning, then print every
     window that is already closed (empty windows included).
  2. Live:    keep consuming; once a second, close and print any window whose
     end is older than (now - GRACE_MIN).

Why a grace period? EMSC publishes an event several minutes after the quake
happens, and revises it ("update") later still. A window is held open for
GRACE_MIN so those reports land in it. Events that arrive after their window
has been printed are reported as LATE and dropped.

Each event appears in Kafka several times (create, updates, backfill), so a
window stores {unid: mag}: duplicates collapse to one event and the most
recent magnitude wins.
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
WINDOW_SEC = 60 * 60  # window size, in event (origin) time
GRACE_MIN = 30        # how long to wait for late reports before printing a window
HISTORY_HOURS = 24 * 7  # ignore events older than this (matches the 7-day backfill)

# Kafka app; offsets are never committed, so every run replays the full history
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="seismic-1h-tumbles",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4", "enable.auto.commit": False},
)

windows = defaultdict(dict)   # window_start (epoch s) -> {unid: mag}
next_emit = None              # start of the oldest window not yet printed


# Start of the 1-hour window that an epoch timestamp falls into
def window_start(ts):
    return int(ts // WINDOW_SEC) * WINDOW_SEC


# Print one window as a single row
def print_window(start, events):
    t0 = datetime.fromtimestamp(start, timezone.utc).strftime("%Y-%m-%d %H:%M")
    t1 = datetime.fromtimestamp(start + WINDOW_SEC, timezone.utc).strftime("%H:%M")
    mags = [m for m in events.values() if m is not None]
    avg = f"{sum(mags) / len(mags):4.2f}" if mags else "  - "
    print(f"{t0}-{t1} UTC | events={len(events):3d} | avg_mag={avg}")


# Print and discard every window that ends at or before `watermark`, in order,
# including empty windows so the report has no gaps
def emit_closed(watermark):
    global next_emit
    if next_emit is None:
        if not windows:
            return
        next_emit = min(windows)
    while next_emit + WINDOW_SEC <= watermark:
        print_window(next_emit, windows.pop(next_emit, {}))
        next_emit += WINDOW_SEC


# Add one Kafka message to its window; return False if it was skipped
def handle(raw):
    try:
        p = json.loads(raw)["data"]["properties"]
        ts = datetime.fromisoformat(p["time"].replace("Z", "+00:00")).timestamp()
    except (KeyError, ValueError, TypeError) as e:
        print(f"  skipping malformed message: {e}")
        return False

    if ts < datetime.now(timezone.utc).timestamp() - HISTORY_HOURS * 3600:
        return False
    start = window_start(ts)
    if next_emit is not None and start < next_emit:
        print(f"  LATE, dropped: {p['unid']} M{p.get('mag')} origin {p['time']} ({p['flynn_region']})")
        return False
    windows[start][p["unid"]] = p.get("mag")
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


# Replay history, print closed windows, then stream live windows until Ctrl-C
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
            print(f"History: {msgs} messages -> {sum(map(len, windows.values()))} distinct events\n")
            emit_closed(now() - GRACE_MIN * 60)

            # Phase 2: keep consuming; close windows by wall clock minus grace
            print(f"\n--- live: each window prints {GRACE_MIN} min after it ends ---\n")
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
