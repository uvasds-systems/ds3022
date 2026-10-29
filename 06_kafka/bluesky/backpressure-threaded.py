import websocket
import json
import queue
import threading
import time
from datetime import datetime
from collections import Counter

"""
This script demonstrates how to use a message queue to buffer incoming 
messages from a realtime data source (like Bluesky Jetstream) and process 
them in parallel using multiple worker threads.

NOTE that it does not publish messages to Kafka, it only buffers them in memory
and simulates some sort of time-consuming processing (see line 72).
"""

# Queue to handle backpressure
# Max 10,000 messages in queue to preserve memory
message_queue = queue.Queue(maxsize=10000)

# Stats tracking
stats = {
    'received': 0,
    'processed': 0,
    'errors': 0,
    'start_time': time.time()
}
stats_lock = threading.Lock()

def on_message(ws, message):
    """Fast: just add to queue"""
    try:
        message_queue.put_nowait(message)
        with stats_lock:
            stats['received'] += 1
    except queue.Full:
        print("⚠️  Queue full! Dropping message (backpressure!)")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print("✓ Connected to Bluesky Jetstream")

def worker(worker_id):
    """Slow: process messages from queue"""
    post_types = Counter()
    
    while True:
        try:
            # Get message from queue
            message = message_queue.get(timeout=1)
            
            # Simulate slow processing (parsing, DB write, API call, etc.)
            data = json.loads(message)
            
            if data.get('kind') == 'commit':
                commit = data.get('commit', {})
                collection = commit.get('collection', 'unknown')
                
                # Extract post text if it's a post
                if collection == 'app.bsky.feed.post':
                    record = commit.get('record', {})
                    text = record.get('text', '')[:50]  # First 50 chars
                    post_types[collection] += 1
                    
                    # Simulate slow work (database write, API call, analysis, etc.)
                    time.sleep(0.01)  # 10ms per message
                    
                    if post_types[collection] % 100 == 0:
                        print(f"[Worker {worker_id}] Processed {post_types[collection]} posts. Latest: {text}...")
            
            with stats_lock:
                stats['processed'] += 1
            
            message_queue.task_done()
            
        except queue.Empty:
            continue
        except json.JSONDecodeError as e:
            with stats_lock:
                stats['errors'] += 1
        except Exception as e:
            print(f"[Worker {worker_id}] Error: {e}")
            with stats_lock:
                stats['errors'] += 1

def stats_reporter():
    """Report stats every 10 seconds"""
    while True:
        time.sleep(10)
        with stats_lock:
            elapsed = time.time() - stats['start_time']
            recv_rate = stats['received'] / elapsed
            proc_rate = stats['processed'] / elapsed
            queue_size = message_queue.qsize()
            
            print(f"\n📊 STATS (after {elapsed:.0f}s):")
            print(f"   Receive rate: {recv_rate:.1f} msg/sec")
            print(f"   Process rate: {proc_rate:.1f} msg/sec")
            print(f"   Queue size: {queue_size}")
            print(f"   Total received: {stats['received']}")
            print(f"   Total processed: {stats['processed']}")
            print(f"   Errors: {stats['errors']}")
            
            if queue_size > 5000:
                print(f"   ⚠️  BACKPRESSURE WARNING: Queue is {queue_size}/10000")

def main():
    # Start worker threads (adjust number based on your processing needs)
    num_workers = 4
    print(f"Starting {num_workers} worker threads...")
    for i in range(num_workers):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
    
    # Start stats reporter
    threading.Thread(target=stats_reporter, daemon=True).start()
    
    # Connect to Bluesky Jetstream
    ws = websocket.WebSocketApp(
        "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )
    
    print("Connecting to Bluesky Jetstream...")
    ws.run_forever()

if __name__ == "__main__":
    main()
