"""
MISSTEP 2: Blocking inside the event loop.

Why it lags:
  asyncio runs everything on ONE thread. A synchronous call inside a coroutine
  (time.sleep, requests.get, a sync database driver) freezes the whole event loop.
  Here a 5 ms "enrichment lookup" runs for every message, capping us at ~200 msg/s
  against a 1000 msg/s source.

Backpressure:
  While the loop is frozen, nothing reads the socket and no keepalive pings are
  answered. Unread messages back up into the socket buffers and the server. The
  connection dies from a keepalive timeout, or the server drops us as too slow.

Watch:  `lag` climbs about 0.8 s every second until the connection drops.
Goal:   do the enrichment without stalling reads from the socket.
"""
import asyncio
import json
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")


def enrich(data):
    """Stand-in for a synchronous lookup, e.g. requests.get("https://geo.example.com/...")."""
    time.sleep(0.005)                                   # <-- THE MISSTEP (blocks the event loop)
    data["enriched"] = True
    return data


async def main():
    """Read trades, enrich each one, and produce it to Kafka."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = enrich(json.loads(message))
                    msg = topic.serialize(key=data["symbol"], value=data)
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value)

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1
                    if count % 1000 == 0:
                        print(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")
        except Exception as e:
            print(f"\nConnection lost: {e!r}")
    print(f"Total produced: {count}, missed: {missed}")


asyncio.run(main())
