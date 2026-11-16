# Why the Original `wikipedia-producer.py` Fell Behind

The original producer probably didn't drop many messages. Its main problem was speed: it could fall behind the stream. It also had one way to lose messages without noticing. This analysis comes from reading the code, not from a measurement.

## Why it fell behind

The stream arrives over a single HTTP connection. Events the script hasn't read yet wait in network buffers. They aren't discarded. The script handled one event at a time, and each event was slow because of this block in `publish_to_kafka()`:

```python
with self.app.get_producer() as producer:
    producer.produce(...)
```

Each pass through that `with` block did three things:

1. It created a new Kafka producer, which connected to the brokers and fetched cluster metadata.
2. It sent one message.
3. On exit, it called `flush()`, which blocked until the broker confirmed that message.

Kafka producers are meant to be created once and reused. `produce()` is normally asynchronous: messages are batched and sent in the background. The original code turned every message into a full connect, send and wait cycle. The two `print()` calls per event added a little more delay.

## What happens when it can't keep up

If the script handles fewer events per second than arrive (about 40/sec on average, more in bursts), a backlog builds and the data in Kafka gets more and more stale. That is a delay, not a loss.

If Wikimedia drops the connection, the `requests_sse` library reconnects on its own. It sends a `Last-Event-Id` header with the last event the script read, and Wikimedia resumes from that point, so the disconnect leaves no gap. The script only stops if every reconnect attempt fails (`max_connect_retry`). If you stop and restart the script yourself, it starts from "now," and anything published in between is missed.

## Where messages could actually be lost

- **Unchecked deliveries.** There was no delivery callback, and the value returned by `flush()` (the number of messages still unsent) was ignored. A message that timed out or was rejected still printed "✓ Published."
- **Failed `produce()` calls.** The error was printed and the event was skipped, with no retry.

## How to confirm it

- Print the lag for each event: the current time minus the event's own `timestamp`. If the lag keeps growing, the producer is behind.
- Alternatively, run `wikipedia-io.py` alongside it and compare its count with the number of messages written to the `wikipedia-edits` topic over the same period.

## The fix (now in `wikipedia-producer.py`)

1. **Open the producer once** and keep it for the whole run. `produce()` returns immediately, the client batches messages in the background, and `flush()` runs only once, at shutdown:

   ```python
   with self.app.get_producer() as producer:      # opened once
       with EventSource(WIKIPEDIA_SSE_URL, headers=headers) as source:
           for event in source:
               producer.produce(..., on_delivery=self.on_delivery)  # async
   ```

2. **Pass an `on_delivery` callback.** The client calls it after the broker confirms or rejects each message, so failures are counted and printed rather than assumed to be successes.

3. **Print a periodic summary** every 5 seconds instead of two lines per event. It shows the rate, the produced, delivered and failed counts, and the current lag behind real time. A steady lag of a few seconds is normal. A lag that keeps growing means the producer can't keep up.
