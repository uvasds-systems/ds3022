"""
MISSTEP 5: Reconnecting without a resume cursor.

Note: unlike 1-4, this is not a backpressure mistake. It is a recovery mistake:
the question is what happens after the connection drops, not how to keep up.
It's included because it hides the damage from 1-4, and because resuming correctly
creates a burst of backlog that tests everything learned there.

Why it loses data:
  Connections drop: network blips, server restarts, or a misstep from 1-4. This script
  catches the error, waits 1 s, and reconnects, which looks robust. But it reconnects
  to the live end of the stream. Everything the server sent while we were away, plus
  anything still unread in the socket, is skipped. The server keeps a 60 s replay
  buffer and accepts  ?cursor=<seq>  to resume, but this script never uses it.

Lag and backpressure:
  `lag` stays near 0 after every reconnect, so the stream looks healthy. That's the
  trap: jumping to live hides the problem instead of fixing it. Resuming correctly
  means replaying a backlog in a burst, and the producer has to absorb that burst.

Watch:  `missed` jumps by ~1000+ after each simulated drop while `lag` stays low.
Goal:   reconnect with no gaps in `seq`, and catch up after a reconnect.
"""
import asyncio
import json
import os
import random
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
DROP_CHANCE = 0.0005                                    # per message: a simulated drop every ~2 s

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")


async def main():
    """Read trades and produce them to Kafka, reconnecting forever on any error (Ctrl+C to stop)."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:
        while True:
            try:
                async with websockets.connect(URL) as ws:   # <-- THE MISSTEP (no ?cursor=, always starts live)
                    async for message in ws:
                        data = json.loads(message)
                        msg = topic.serialize(key=data["symbol"], value=data)
                        producer.produce(topic=topic.name, key=msg.key, value=msg.value)

                        # Bookkeeping: count gaps in `seq` and how far behind real time we are
                        if last_seq is not None and data["seq"] != last_seq + 1:
                            missed += data["seq"] - last_seq - 1
                        last_seq, count = data["seq"], count + 1
                        if count % 1000 == 0:
                            print(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")

                        if random.random() < DROP_CHANCE:
                            raise ConnectionError("simulated network drop")
            except Exception as e:
                print(f"Connection lost: {e!r}; reconnecting in 1 s")
                await asyncio.sleep(1)


asyncio.run(main())
