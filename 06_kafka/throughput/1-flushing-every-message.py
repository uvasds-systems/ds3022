"""
MISSTEP 1: Flushing after every message.

Why it lags:
  producer.produce() is asynchronous. It puts the message in a local buffer, and the
  Kafka client sends buffered messages to the brokers in batches. Calling flush()
  after every produce() waits for the brokers to acknowledge that one message before
  the next is read. Every message costs a full network round trip, so throughput tops
  out around 1 / round-trip-time, below the source's 1000 msg/s.

Backpressure:
  While flush() waits, nothing reads the WebSocket. Unread messages pile up in the OS
  socket buffers, then on the server. Once we fall more than the server's replay
  window (60 s) behind, it disconnects us.

Watch:  `lag` in the output climbs steadily until the connection drops.
Goal:   keep up with the source (lag near 0) while still delivering every message.
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


async def main():
    """Read trades from the WebSocket and produce each one to Kafka."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    msg = topic.serialize(key=data["symbol"], value=data)
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value)
                    producer.flush()                    # <-- THE MISSTEP (waits for an ack every message)

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
