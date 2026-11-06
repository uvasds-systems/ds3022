#!/usr/bin/env python3

"""
Tumbling Window Consumer

Reads sensor events from "sensor-events" and groups them by hand into
10-second tumbling windows of event time. A watermark (max event time seen
minus a 5 s grace period) decides when a window closes and is printed.
Events that arrive after their window has closed are dropped.
"""

import os
import json
from collections import defaultdict
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "sensor-events"
WINDOW_SEC = 10   # window size, in event time
GRACE_SEC = 5     # how long to wait for stragglers

# Kafka app; a new consumer group starts from the oldest message in the topic
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="tumbling-demo",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4"},
)

windows = defaultdict(list)   # window_start -> list of values
max_event_ts = 0.0            # highest event time seen so far


# Start of the tumbling window that a timestamp falls into
def window_start(ts):
    return int(ts // WINDOW_SEC) * WINDOW_SEC


# Print and discard every window whose end is at or behind the watermark
def emit_closed_windows(watermark):
    for start in sorted(windows):
        end = start + WINDOW_SEC
        if end <= watermark:
            vals = windows.pop(start)
            avg = sum(vals) / len(vals)
            print(f"[{start:.0f}, {end:.0f})  count={len(vals):3d}  avg={avg:6.4f}")


# Assign one event to its window, or drop it if that window already closed
def handle_event(event):
    global max_event_ts
    ts, val = event["ts"], event["value"]
    max_event_ts = max(max_event_ts, ts)
    watermark = max_event_ts - GRACE_SEC

    if window_start(ts) + WINDOW_SEC <= watermark:
        print(f"  LATE, dropped: event_ts={ts:.2f}")
        return

    windows[window_start(ts)].append(val)
    emit_closed_windows(watermark)


# Poll the topic indefinitely until Ctrl-C
def consume_events():
    try:
        with app.get_consumer() as consumer:
            consumer.subscribe([TOPIC])
            print(f"Consuming from topic '{TOPIC}' on {KAFKA_BROKER}")
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                handle_event(json.loads(msg.value()))
                # Quix disables auto offset storing; mark this message as processed
                consumer.store_offsets(message=msg)
    except KeyboardInterrupt:
        print("Stopped")
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")


if __name__ == "__main__":
    consume_events()
