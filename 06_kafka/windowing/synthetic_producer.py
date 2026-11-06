#!/usr/bin/env python3

"""
Windowing Producer

Sends 1000 sensor readings to the "sensor-events" topic, one every 0.5 s.
About 10% of events are "late": their event time is up to 8 s in the past.
The event time is also set as the Kafka message timestamp, so quixstreams
windows on the consumer side group by event time, not arrival time.
"""

import os
import random
import time
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "sensor-events"
NUM_EVENTS = 1000
SENSOR_ID = "sensor-1"

# Kafka app and topic; force IPv4 to avoid localhost resolving to IPv6
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic(name=TOPIC, value_serializer="json")


# Emit NUM_EVENTS readings, occasionally backdating the event time
def produce_events():
    try:
        with app.get_producer() as producer:
            start = time.time()
            for i in range(NUM_EVENTS):
                lateness = random.uniform(-8, 0) if random.random() < 0.10 else 0.0
                event_ts = start + i * 0.5 + lateness
                value = {"ts": event_ts, "value": random.gauss(20, 3)}

                # Kafka timestamps are in milliseconds
                msg = topic.serialize(key=SENSOR_ID, value=value)
                producer.produce(
                    topic=topic.name,
                    key=msg.key,
                    value=msg.value,
                    timestamp=int(event_ts * 1000),
                )
                print(f"{'LATE ' if lateness else ''}{value}")
                time.sleep(0.5)
        print(f"Produced {NUM_EVENTS} events to '{TOPIC}'")
    except Exception as e:
        print(f"Error producing to Kafka: {e}")


if __name__ == "__main__":
    produce_events()
