# Why `wikipedia-producer.py` Falls Behind

The producer most likely isn't dropping messages. It is falling behind the stream, and that can turn into lost messages later. (This is from reading the code, not from a measurement.)

## Why it falls behind

The stream arrives over a single HTTP connection. Events the script hasn't read yet wait in network buffers on both ends; they aren't discarded. The script handles one event at a time, and each event is slow because of this block in `publish_to_kafka()`:

```python
with self.app.get_producer() as producer:
    producer.produce(...)
```

Each pass through that `with` block does three things:

1. It creates a new Kafka producer, which connects to the brokers and fetches cluster metadata.
2. It sends one message.
3. On exit, it calls `flush()`, which blocks until the broker confirms that message.

Kafka producers are meant to be created once and reused. `produce()` is normally asynchronous: messages are batched and sent in the background. This code turns every message into a full connect, send and wait cycle. The two `print()` calls per event add a little more delay.

## What happens when it can't keep up

If the script handles fewer events per second than the ~40/sec arriving, the backlog keeps growing. The data in Kafka gets more and more stale. Wikimedia may eventually close the connection to a slow client. When that happens, the `for` loop just ends and the script exits without reconnecting, and everything still buffered is lost. Restarting it starts from "now," so the gap is never recovered.

There is also one direct form of loss now: if a `produce()` fails, the error is printed and that event is skipped.

## How to confirm it

- Print the lag for each event: the current time minus the event's own `timestamp`. If the lag keeps growing, the producer is behind.
- Alternatively, run `wikipedia-io.py` alongside it and compare its count with the number of messages written to the `wikipedia-edits` topic over the same period.

## Simple fix

Create the producer once and keep it open for the whole run:

```python
with self.app.get_producer() as producer:      # opened once
    with EventSource(WIKIPEDIA_SSE_URL, headers=headers) as source:
        for event in source:
            ...
            producer.produce(topic=..., key=..., value=...)   # async, no flush
```

Each `produce()` then returns right away, the Kafka client batches messages in the background, and `flush()` runs only once, at shutdown. Cutting the per-event prints to a periodic summary would help a little more.

For more robustness, you could also reconnect when the stream drops, passing the last event ID so Wikimedia resumes where it left off. That's optional for a class demo.
