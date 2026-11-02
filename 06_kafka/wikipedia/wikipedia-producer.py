#!/usr/bin/env python3

"""
Wikipedia Recent Changes Stream Producer

This script connects to Wikipedia's SSE stream for recent changes
and produces them to a Kafka topic for processing.
"""

from quixstreams import Application
import os
import json
from requests_sse import EventSource
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WIKIPEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


class WikipediaStreamer:
    def __init__(self):
        """Initialize the Wikipedia streamer with Kafka producer."""
        self.app = Application(
            broker_address=KAFKA_BROKER,
            consumer_group="wikipedia-producer",
            producer_extra_config={
                # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
                "broker.address.family": "v4",
            }
        )
        self.topic = self.app.topic(
            name="wikipedia-edits",
            value_serializer="json",
        )
        print(f"Connected to Kafka broker at {KAFKA_BROKER}")
        print(f"Producing to topic: {self.topic.name}")

    def publish_to_kafka(self, change_event: dict) -> bool:
        """
        Publish a Wikipedia change event to Kafka.
        
        Args:
            change_event: Dictionary containing the change event data
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Use the event ID as the key for partitioning
            event_id = str(change_event.get('id', ''))
            
            # Serialize the event
            serialized = self.topic.serialize(key=event_id, value=change_event)
            
            # Produce to Kafka
            with self.app.get_producer() as producer:
                producer.produce(
                    topic=self.topic.name,
                    key=serialized.key,
                    value=serialized.value
                )
            return True
            
        except Exception as e:
            print(f"Error publishing to Kafka: {e}")
            return False

    def process_change(self, change_event: dict) -> None:
        """
        Process a single Wikipedia change event.
        
        Args:
            change_event: Dictionary containing the change event data
        """
        # Extract key information
        event_type = change_event.get('type', 'unknown')
        title = change_event.get('title', 'N/A')
        user = change_event.get('user', 'anonymous')
        wiki = change_event.get('server_name', 'unknown')
        timestamp = change_event.get('timestamp')
        
        # Print the change
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [{event_type}] {user} edited '{title}' on {wiki}")
        
        # Publish to Kafka
        if self.publish_to_kafka(change_event):
            print(f"  ✓ Published to Kafka (ID: {change_event.get('id')})")

    def run(self):
        """
        Connect to Wikipedia SSE stream and continuously process events.
        """
        print(f"Connecting to Wikipedia SSE stream: {WIKIPEDIA_SSE_URL}")
        print("Press Ctrl+C to stop\n")
        
        try:
            # Set up headers with proper User-Agent (required by Wikimedia)
            headers = {
                'User-Agent': 'WikipediaStreamer/1.0 (Educational Project; Python/requests-sse)',
                'Accept': 'text/event-stream'
            }
            
            # Connect to the SSE stream with headers
            with EventSource(WIKIPEDIA_SSE_URL, headers=headers) as source:
                for event in source:
                    # The message data is in event.data as a JSON string
                    if event.data:
                        try:
                            change_event = json.loads(event.data)
                            self.process_change(change_event)
                        except json.JSONDecodeError as e:
                            print(f"Error parsing event: {e}")
                        except Exception as e:
                            print(f"Error processing event: {e}")
                            
        except KeyboardInterrupt:
            print("\n\nStopping stream...")
        except Exception as e:
            print(f"Error connecting to stream: {e}")
            raise


if __name__ == "__main__":
    streamer = WikipediaStreamer()
    streamer.run()
