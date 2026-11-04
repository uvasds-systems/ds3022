"""
MISSTEP 3: An unbounded asyncio.Queue between the socket and the producer.

Why it lags:
  A fast receiver task drops every message into a queue, and a slower writer task
  (~500 msg/s here) pulls from it and produces to Kafka. The writer can't keep up
  with the 1000 msg/s source, so the queue grows by ~500 messages every second.

Backpressure (or the lack of it):
  With no maxsize, queue.put() never waits, so the receiver never slows down. The
  socket always looks drained, and the server never sees us as slow. The lag hasn't
  gone away; it has moved into our own memory, where nothing pushes back. When the
  process runs out of memory (simulated at MEMORY_LIMIT), everything still queued is
  lost. The server already sent those messages, so they can't be replayed.

Watch:  `queued` and `lag` grow without limit until the simulated crash.
Goal:   make a slow writer slow down the reader instead of growing memory, and make
        the writer faster.
"""
import asyncio
import json
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
MEMORY_LIMIT = 50_000                                   # simulated OOM threshold (queued messages)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")
queue = asyncio.Queue()                                 # <-- THE MISSTEP (no maxsize)


async def receiver():
    """Drain the socket as fast as messages arrive; put() never waits."""
    async with websockets.connect(URL) as ws:
        async for message in ws:
            await queue.put(message)
            if queue.qsize() > MEMORY_LIMIT:
                raise MemoryError(f"queue reached {queue.qsize():,} messages")


async def writer(producer):
    """Pull from the queue and produce to Kafka, slower than the source."""
    count = 0
    while True:
        data = json.loads(await queue.get())
        await asyncio.sleep(0.002)                      # slow downstream work (~500 msg/s)
        msg = topic.serialize(key=data["symbol"], value=data)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        count += 1
        if count % 1000 == 0:
            print(f"produced={count} queued={queue.qsize():,} lag={time.time() - data['ts']:.2f}s")


async def main():
    """Run the receiver and writer side by side until the simulated crash."""
    with app.get_producer() as producer:
        task = asyncio.create_task(writer(producer))
        try:
            await receiver()
        except Exception as e:
            print(f"\nCrashed: {e!r}")
            print(f"LOST {queue.qsize():,} queued messages (already sent by the server, cannot replay)")
        finally:
            task.cancel()


asyncio.run(main())
