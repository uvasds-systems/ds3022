import websocket
import json
from datetime import datetime

"""
This script simply prints the rate of messages received from Bluesky Jetstream.
It performs no processing of data.
"""

message_count = 0
start_time = datetime.now()

print(">>> Bluesky Jetstream throughput:")

def on_message(ws, message):
    global message_count
    message_count += 1
    
    elapsed = (datetime.now() - start_time).total_seconds()
    if elapsed > 0 and message_count % 100 == 0:
        print(f"Rate: {message_count/elapsed:.2f} msg/sec")

ws = websocket.WebSocketApp(
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    on_message=on_message
)
ws.run_forever()
