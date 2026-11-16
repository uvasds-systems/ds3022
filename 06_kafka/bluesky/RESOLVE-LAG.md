# Resolving Consumer Lag

When the consumer's lag keeps growing ("Lagging 👎"), it is processing messages more slowly
than the producer writes them. The Bluesky producer runs at roughly 500 msg/sec, which a
single Python consumer should normally keep up with. So measure first: compare the
consumer's own msg/sec against the producer's rate before changing anything. Possible causes
include `loglevel="DEBUG"`, or starting the consumer late so it begins with a backlog.

These are the four strongest fixes, roughly in the order to try them.

## 1. Read messages in batches instead of one at a time

`consumer.poll(1)` returns a single message per call, and Python pays a fixed overhead on
every call. This is usually the bottleneck.

```python
msgs = consumer.consume(num_messages=500, timeout=1)
for msg in msgs:
    ...
```

Batches of up to 500 messages often raise throughput 5–20x. It's a small change to the
loop, and the code stays in one file with one consumer. **Best first fix.**

## 2. Do less work per message

The consumer parses each full event (often several KB of JSON) just to read
`commit.operation`. Two ways to cut that:

- Use a faster JSON parser such as `orjson`, typically 3–5x faster than `json`.
- Have the producer put the operation in a Kafka message header, so the consumer can count
  without parsing the event body at all.

Also batch Redis writes. One `INCR` per message is a network round trip each time;
collect counts in a local `Counter` and write them in a single pipeline every few seconds.

## 3. Add partitions and run more consumers

A topic with `num_partitions=1` can only be read by one consumer in a group. Recreate the
topic with more partitions (e.g. 3–6, set in the producer's `TopicConfig`) and start more
copies of the consumer under the same `consumer_group`. Kafka then splits the partitions
among them.

Messages are keyed by DID, so each user's events still stay in order within a partition.
This is the standard way to scale out, but it means recreating the topic and running
several processes.

## 4. Use quixstreams' StreamingDataFrame

Replace the manual consumer loop with `app.dataframe()` + `app.run()`. The library then
handles batching, commits, and shutdown. Counts can live in its built-in state store or be
flushed to Redis.

This is the cleanest long-term design, but it takes the most rewriting and hides the offset
mechanics the manual loop exposes.

## Recommendation

Start with **#1**. If lag still grows, or to practise scaling out, move on to **#3**.
