#!/usr/bin/env python3

"""
Seismic Producer (no-loss version)

Streams EMSC earthquake events from the seismic portal websocket into the
"seismic" Kafka topic, keyed by event id (unid).

Where messages can be lost, and what this script does about each:
  1. Websocket drops (the server closes idle/old connections, networks blip):
     reconnect with exponential backoff, forever.
  2. Events published while we were disconnected: on every (re)connect,
     backfill from the EMSC FDSN REST API with `updatedafter=<last seen>`.
     Duplicates are harmless; consumers dedupe on unid.
  3. Kafka write failures: idempotent producer (acks=all, quix default) plus a
     delivery callback that reports any message the brokers did not accept.
  4. Ctrl-C with messages still buffered: the producer context manager
     flushes the local queue before exiting.
"""

import os
import json
import asyncio
from datetime import datetime, timedelta, timezone
from quixstreams import Application
from tornado.httpclient import AsyncHTTPClient
from tornado.websocket import websocket_connect

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "seismic"
WSS_URI = "wss://www.seismicportal.eu/standing_order/websocket"
FDSN_URI = "https://www.seismicportal.eu/fdsnws/event/1/query"
BACKFILL_HOURS = 24      # on a cold start, how far back to backfill (matches topic retention)
MAX_BACKOFF_SEC = 60     # cap on the reconnect delay

# Kafka app; force IPv4 to avoid localhost resolving to IPv6
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic(name=TOPIC, value_serializer="json")

stats = {"sent": 0, "acked": 0, "failed": 0}
last_update = datetime.now(timezone.utc) - timedelta(hours=BACKFILL_HOURS)  # backfill watermark


# Called by librdkafka once the brokers accept (or reject) each message
def on_delivery(err, msg):
    if err:
        stats["failed"] += 1
        print(f"DELIVERY FAILED key={msg.key()}: {err}")
    else:
        stats["acked"] += 1


# Produce one event ({"action": ..., "data": <GeoJSON feature>}) keyed by unid
def produce(producer, event):
    global last_update
    try:
        p = event["data"]["properties"]
        msg = topic.serialize(key=p["unid"], value=event)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value, on_delivery=on_delivery)
        stats["sent"] += 1
        # Remember the newest lastupdate so a reconnect only backfills the gap
        last_update = max(last_update, datetime.fromisoformat(p["lastupdate"].replace("Z", "+00:00")))
        print(f"{event['action']:8} | {p['time']} | M{p['mag']} | {p['flynn_region']}")
    except (KeyError, ValueError) as e:
        print(f"Skipping malformed event ({e}): {str(event)[:200]}")


# Pull every event updated since `last_update` from the REST API and produce it
async def backfill(producer):
    since = (last_update - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S")  # 1 min overlap
    url = f"{FDSN_URI}?format=json&updatedafter={since}&limit=20000"
    try:
        resp = await AsyncHTTPClient().fetch(url, request_timeout=60, raise_error=False)
        if resp.code == 204:  # FDSN: no matching events
            print(f"Backfill since {since}: 0 events")
            return
        resp.rethrow()
        features = json.loads(resp.body)["features"]
        # Oldest first so per-key order in Kafka matches update order
        for f in sorted(features, key=lambda f: f["properties"]["lastupdate"]):
            produce(producer, {"action": "backfill", "data": f})
        print(f"Backfill since {since}: {len(features)} events")
    except Exception as e:
        print(f"Backfill failed, continuing with live stream: {e}")


# Connect, backfill the gap, then stream live; reconnect whenever the socket closes
async def main():
    print(f"Producing to '{TOPIC}' on {KAFKA_BROKER}. Press Ctrl+C to stop\n")
    backoff = 1
    with app.get_producer() as producer:
        while True:
            try:
                ws = await websocket_connect(WSS_URI, ping_interval=15)
                print(f"Connected to {WSS_URI}")
                await backfill(producer)
                backoff = 1
                while (message := await ws.read_message()) is not None:
                    try:
                        produce(producer, json.loads(message))
                    except json.JSONDecodeError as e:
                        print(f"Bad JSON from websocket: {e}")
                print("Websocket closed by server")
            except Exception as e:
                print(f"Websocket error: {e}")

            # Serve pending delivery callbacks, then wait before reconnecting
            producer.poll(0)
            print(f"Reconnecting in {backoff}s | sent={stats['sent']} acked={stats['acked']} failed={stats['failed']}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SEC)


if __name__ == "__main__":
    try:
        # asyncio.run cancels main() on Ctrl-C, so the producer's `with` block exits and flushes
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\nStopped | sent={stats['sent']} acked={stats['acked']} failed={stats['failed']}")
