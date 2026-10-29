import websocket
import json
import os
from datetime import datetime
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
This script demonstrates how to use a Kafka producer to stream real-time 
events from Bluesky Jetstream to a Kafka topic.

NOTE that it does not parse/consume messages, it publishes/produces them to Kafka.
"""

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        # Batch settings for better throughput
        "linger.ms": 5,  # Wait up to 5ms to batch messages
        "batch.size": 100000,  # Batch up to 100KB
    }
)
topic = app.topic(
    name="bluesky-events",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=1, 
        replication_factor=3,
    )
)

# Create a kafka producer 
producer = app.get_producer()

message_count = 0
start_time = datetime.now()

def on_message(ws, message):
    # set up a counter
    global message_count
    message_count += 1
    
    data = json.loads(message)
    rev = data['commit']['rev']
    serialized = topic.serialize(key=rev, value=data)
    
    # Reuse the same producer - no context manager!
    producer.produce(
        topic=topic.name,
        key=serialized.key,
        value=serialized.value
    )

    # Print rate every 100 messages
    if message_count % 100 == 0:
        elapsed = (datetime.now() - start_time).total_seconds()
        if elapsed > 0:
            print(f"Rate: {message_count/elapsed:.2f} msg/sec (total: {message_count})")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed, flushing remaining messages...")
    producer.flush()

ws = websocket.WebSocketApp(
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    on_message=on_message,
    on_close=on_close
)

try:
    ws.run_forever()
finally:
    # Ensure we flush on exit
    producer.flush()
    print(f"\nTotal messages produced: {message_count}")
