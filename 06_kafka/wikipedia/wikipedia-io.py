import json
import time
from collections import Counter
from requests_sse import EventSource

"""
This script prints the rate of messages received from the Wikipedia
recent changes SSE stream over fixed 5-second windows, with a per-type
breakdown. It performs no processing of data.
"""

WIKIPEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
WINDOW_SECONDS = 5

# Wikimedia requires a descriptive User-Agent
HEADERS = {
    "User-Agent": "WikipediaStreamer/1.0 (Educational Project; Python/requests-sse)",
    "Accept": "text/event-stream",
}

total = 0
window_types = Counter()
window_start = time.monotonic()

print(f">>> Wikipedia recent changes throughput ({WINDOW_SECONDS}s windows):")

try:
    with EventSource(WIKIPEDIA_SSE_URL, headers=HEADERS) as source:
        for event in source:
            # Skip keep-alive / empty events
            if not event.data:
                continue
            total += 1

            # Tally by event type (edit, categorize, log, new, ...)
            try:
                window_types[json.loads(event.data).get("type", "unknown")] += 1
            except json.JSONDecodeError:
                window_types["unparseable"] += 1

            # At the end of each window, print its rate and reset the counters
            elapsed = time.monotonic() - window_start
            if elapsed >= WINDOW_SECONDS:
                count = sum(window_types.values())
                breakdown = ", ".join(f"{t}={c}" for t, c in window_types.most_common())
                print(f"Rate: {count/elapsed:6.2f} msg/sec  ({breakdown})")
                window_types.clear()
                window_start = time.monotonic()
except KeyboardInterrupt:
    print(f"\nStopped after {total} messages.")
except Exception as e:
    print(f"Stream error: {e}")
