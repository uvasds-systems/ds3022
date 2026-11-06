#!/usr/bin/env python3

"""
Seismic Backfill (one-shot)

Fetches every EMSC earthquake whose ORIGIN time falls in the last 7 days from
the FDSN REST API, produces each one to the "seismic-backfill" Kafka topic
(keyed by unid), flushes, and exits. It does not open the websocket, so no
live events are captured.

Messages are timestamped at produce time, not quake time, so topic retention
counts from when this script runs.
"""

import os
import requests
from datetime import datetime, timedelta, timezone
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "seismic-backfill"
FDSN_URI = "https://www.seismicportal.eu/fdsnws/event/1/query"
BACKFILL_HOURS = 24 * 7   # how far back (by quake origin time) to fetch

# Kafka app; force IPv4 to avoid localhost resolving to IPv6
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic(name=TOPIC, value_serializer="json")

stats = {"sent": 0, "acked": 0, "failed": 0}


# Called by librdkafka once the brokers accept (or reject) each message
def on_delivery(err, msg):
    if err:
        stats["failed"] += 1
        print(f"DELIVERY FAILED key={msg.key()}: {err}")
    else:
        stats["acked"] += 1


# Produce one event ({"action": ..., "data": <GeoJSON feature>}) keyed by unid
def produce(producer, event):
    try:
        p = event["data"]["properties"]
        msg = topic.serialize(key=p["unid"], value=event)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
        stats["sent"] += 1
        print(f"{event['action']:8} | {p['time']} | M{p['mag']} | {p['flynn_region']}")
    except (KeyError, ValueError) as e:
        print(f"Skipping malformed event ({e}): {str(event)[:200]}")


# Fetch all events with origin time >= now - BACKFILL_HOURS; returns a list of GeoJSON features
def fetch_events():
    since = (datetime.now(timezone.utc) - timedelta(hours=BACKFILL_HOURS)).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Fetching events with origin time since {since} UTC")
    resp = requests.get(FDSN_URI, params={"format": "json", "starttime": since, "limit": 20000}, timeout=120)
    if resp.status_code == 204:  # FDSN: no matching events
        return []
    resp.raise_for_status()
    return resp.json()["features"]


# Fetch, produce oldest-first, then flush (on exiting the producer block) and exit
def main():
    try:
        features = fetch_events()
        print(f"Fetched {len(features)} events; producing to '{TOPIC}' on {KAFKA_BROKER}\n")
        with app.get_producer() as producer:
            for f in sorted(features, key=lambda f: f["properties"]["time"]):
                produce(producer, {"action": "backfill", "data": f})
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"Backfill failed: {e}")
    print(f"Done | sent={stats['sent']} acked={stats['acked']} failed={stats['failed']}")


if __name__ == "__main__":
    main()
