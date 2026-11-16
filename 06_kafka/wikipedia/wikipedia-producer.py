#!/usr/bin/env python3

"""
Wikipedia Recent Changes Stream Producer

This script connects to Wikipedia's SSE stream for recent changes
and produces them to a Kafka topic for processing.

A single Kafka producer is opened for the whole run, so produce() is
asynchronous and batched. Delivery failures are reported through a
callback, and a summary (rate, failures, lag) is printed periodically.
"""

from quixstreams import Application
import os
import json
import time
from requests_sse import EventSource

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WIKIPEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
REPORT_SECONDS = 5

# Wikimedia requires a descriptive User-Agent
HEADERS = {
    "User-Agent": "WikipediaStreamer/1.0 (Educational Project; Python/requests-sse)",
    "Accept": "text/event-stream",
}


class WikipediaStreamer:
    def __init__(self):
        """Initialize the Kafka application, topic, and counters."""
        self.app = Application(
            broker_address=KAFKA_BROKER,
            producer_extra_config={
                # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
                "broker.address.family": "v4",
            },
        )
        self.topic = self.app.topic(name="wikipedia-edits", value_serializer="json")

        # Counters: events produced, confirmed by the broker, and failed
        self.produced = self.delivered = self.failed = 0
        self.window_count = 0
        self.window_start = time.monotonic()
        self.last_lag = 0.0

        print(f"Kafka broker(s): {KAFKA_BROKER}")
        print(f"Producing to topic: {self.topic.name}")

    def on_delivery(self, err, msg) -> None:
        """Called by the Kafka client once the broker confirms (or rejects) a message."""
        if err is not None:
            self.failed += 1
            print(f"  ✗ Delivery failed (key={msg.key()}): {err}")
        else:
            self.delivered += 1

    def publish(self, producer, change_event: dict) -> None:
        """Queue one change event for delivery to Kafka (non-blocking)."""
        try:
            # Use the event ID as the key so events spread across partitions
            serialized = self.topic.serialize(key=str(change_event.get("id", "")), value=change_event)
            producer.produce(
                topic=self.topic.name,
                key=serialized.key,
                value=serialized.value,
                on_delivery=self.on_delivery,
            )
            self.produced += 1
            self.window_count += 1
        except Exception as e:
            self.failed += 1
            print(f"  ✗ Produce error: {e}")

        # Lag: how far behind real time this event is (Wikimedia timestamps are Unix seconds)
        ts = change_event.get("timestamp")
        if isinstance(ts, (int, float)):
            self.last_lag = time.time() - ts

    def report(self) -> None:
        """Print a periodic summary instead of a line per event."""
        elapsed = time.monotonic() - self.window_start
        if elapsed < REPORT_SECONDS:
            return
        print(
            f"Rate: {self.window_count / elapsed:6.2f} msg/sec | produced={self.produced} "
            f"delivered={self.delivered} failed={self.failed} | lag={self.last_lag:5.1f}s"
        )
        self.window_count = 0
        self.window_start = time.monotonic()

    def run(self) -> None:
        """
        Connect to the Wikipedia SSE stream and produce every event to Kafka.

        requests_sse reconnects automatically if the stream drops, resuming
        from the last event ID it read, so a brief disconnect leaves no gap.
        """
        print(f"Connecting to Wikipedia SSE stream: {WIKIPEDIA_SSE_URL}")
        print("Press Ctrl+C to stop\n")

        try:
            # Open the producer once; its __exit__ flushes queued messages at shutdown
            with self.app.get_producer() as producer:
                with EventSource(WIKIPEDIA_SSE_URL, headers=HEADERS) as source:
                    for event in source:
                        # Skip keep-alive / empty events
                        if not event.data:
                            continue
                        try:
                            self.publish(producer, json.loads(event.data))
                        except json.JSONDecodeError as e:
                            print(f"Error parsing event: {e}")
                        self.report()
        except KeyboardInterrupt:
            print("\nStopping stream (flushing queued messages)...")
        except Exception as e:
            print(f"Stream error: {e}")
            raise
        finally:
            print(f"Final: produced={self.produced} delivered={self.delivered} failed={self.failed}")


if __name__ == "__main__":
    WikipediaStreamer().run()
