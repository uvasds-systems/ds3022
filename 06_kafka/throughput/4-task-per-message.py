"""
MISSTEP 4: Spawning a task per message.

Why it lags:
  Each trade needs an async call to a downstream enrichment service. To avoid
  waiting on it, the loop starts a new task per message with asyncio.create_task()
  and moves on. But the service only handles 10 requests at a time at 20 ms each,
  or ~500 req/s, against a 1000 msg/s source. The extra ~500 tasks per second wait
  for a free slot, and the number of tasks in flight grows without limit.

Backpressure (or the lack of it):
  create_task() never waits, so the reader never slows down, and the socket always
  looks drained. Like misstep 3, the backlog moves into our own memory, but this time
  as thousands of waiting tasks instead of a visible queue. When memory runs out
  (simulated at MEMORY_LIMIT), every unfinished task's message is lost.

Watch:  `in_flight` and `lag` grow without limit until the simulated crash.
Goal:   limit how much work is in flight, so a slow downstream slows the reader.
"""
import asyncio
import json
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
MEMORY_LIMIT = 50_000                                   # simulated OOM threshold (tasks in flight)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")
downstream = asyncio.Semaphore(10)                      # the enrichment service: 10 concurrent requests
pending = set()                                         # tasks still running (for counting)
stats = {"produced": 0, "lag": 0.0}


async def handle(producer, data):
    """Enrich one trade through the downstream service, then produce it to Kafka."""
    async with downstream:
        await asyncio.sleep(0.02)                       # simulated 20 ms async HTTP call
    data["enriched"] = True
    msg = topic.serialize(key=data["symbol"], value=data)
    producer.produce(topic=topic.name, key=msg.key, value=msg.value)
    stats["produced"] += 1
    stats["lag"] = time.time() - data["ts"]


async def main():
    """Read trades and start one enrichment task per message."""
    count = 0
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    task = asyncio.create_task(handle(producer, json.loads(message)))  # <-- THE MISSTEP
                    pending.add(task)
                    task.add_done_callback(pending.discard)

                    count += 1
                    if count % 1000 == 0:
                        print(f"received={count} produced={stats['produced']} "
                              f"in_flight={len(pending):,} lag={stats['lag']:.2f}s")
                    if len(pending) > MEMORY_LIMIT:
                        raise MemoryError(f"{len(pending):,} tasks in flight")
        except Exception as e:
            print(f"\nCrashed: {e!r}")
            print(f"LOST {len(pending):,} in-flight messages (already sent by the server, cannot replay)")
        finally:
            for task in pending:
                task.cancel()


asyncio.run(main())
