# Learn Apache Kafka

<img src="https://us.ovhcloud.com/sites/default/files/styles/desktop_full_width/public/2024-01/kafka.webp" style="align:right;float:right;max-width:40%;" alt="Apache Kafka Logo" />

Learn how to work with Apache Kafka by streaming real-time events from Bluesky and Wikipedia.

## Why Kafka?

Kafka is a distributed log system that excels at handling streams of events. Key benefits:

- **Independent producers and consumers**: Producers write events without waiting for consumers. Multiple consumers can read the same stream at their own pace.
- **Persistent storage**: Events are saved to disk, so data survives crashes and can be replayed later for debugging or reprocessing.
- **Scalable**: Handle high volumes by distributing data across multiple servers and processing with multiple consumers.
- **Reliable**: Data is replicated across servers, so if one fails, others take over automatically.
- **Real-time**: Process events as they arrive with low latency.

This project demonstrates these concepts by streaming events from Bluesky and Wikipedia, then processing them with independent consumers.

## What Goes in Kafka?

Kafka stores **events**—records of something that happened at a specific time. Examples include:
- Real-time events (like Wikipedia edits or Bluesky posts)
- System logs and audit trails
- Metrics and monitoring data
- Messages between services

Events are **immutable** (can't be changed once written), making Kafka great for audit trails, debugging, and replaying data for analysis.

## Basic Concepts

- **Brokers** - Servers that store and distribute messages. Multiple brokers form a cluster.
- **Topics** - Named streams where messages are stored. Topics can be split into partitions and replicated across brokers.
- **Producers** - Applications that write messages to topics (like `wikipedia-producer.py` and `bluesky-producer.py`).
- **Consumers** - Applications that read messages from topics. They track their position and can resume after restarts.
- **Consumer Groups** - Multiple consumers working together to process messages faster. Kafka automatically balances the load among them.

## Getting Started

1. **Create a Python virtual environment** (using `virtualenv` or `pipenv`).
2. **Start Kafka with Docker**:
    ```
    docker compose up -d
    ```
3. **View the Kafka web UI** at http://127.0.0.1:8080/

## Examples

### Wikipedia Event Stream

The Wikipedia stream sends about 2-5 messages per second with 4 event types.

**Run the producer:**
```
python wikipedia-producer.py
```

Let it run for a while, then check the new topic in the Kafka UI.

**Run the consumer:**
```
python wikipedia-consumer.py
```

Let it process messages for a few minutes, then press Ctrl+C to see the event counts by type (stored in Redis).

### Bluesky Firehose

The Bluesky stream is higher volume, typically 40-100 messages per second with 3 event types (`create`, `delete`, `update`).

**Run the producer:**
```
python bluesky-producer.py
```

**Run the consumer in a separate terminal:**
```
python bluesky-consumer.py
```

Press Ctrl+C to stop and view the event type counts.

### Backpressure

Backpressure occurs when your data source (like Bluesky) sends messages faster than your producer can write them to Kafka. Messages accumulate in memory or get lost, which is problematic.

**Common solutions:**

1. **Batch messages** - Group multiple messages together before sending (see `bluesky-producer.py` for batching configuration).
2. **Use a message queue** - Buffer incoming messages in memory while sending to Kafka (see `backpressure-threaded.py` for an example).
3. **Parallel processing** - Use multiple threads or processes to send messages concurrently.
4. **Optimize settings** - Tune producer configuration for better throughput (buffer sizes, compression, etc.).

The example scripts demonstrate these techniques. Monitor message rates to detect when backpressure occurs.

## Cleaning Up

Stop the Kafka cluster when you're done:

```
docker compose down
```

