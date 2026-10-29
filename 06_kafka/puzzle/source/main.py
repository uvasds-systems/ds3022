#!/usr/bin/env python3

"""
Scrambled Phrase WebSocket Server

A connected client receives one "cycle": every character of PHRASE (spaces included)
as a JSON message, sent in a shuffled order as fast as possible after an
initial 30 second delay. Once a full cycle has been delivered, the server shuts down
(and the container exits). A client that disconnects mid-cycle can reconnect for a fresh one.
Students must capture the messages, publish them to Kafka, then consume and
reassemble the phrase by sorting on order_no.

Message format:
    {"id": "1a2b3c", "cycle_id": "9f8e7d", "letter": "q", "order_no": 5, "total": 44}
"""

import asyncio
import json
import os
import random
import secrets

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

PHRASE = "Do you want to know who you are? Don't ask. Act! Action will delineate and define you."
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8765"))
START_DELAY = 30    # seconds to wait after a client connects before the first message


def build_cycle(phrase: str) -> list[dict]:
    """Return one message per character (1-based order_no), shuffled."""
    cycle_id = secrets.token_hex(3)
    messages = [
        {"id": secrets.token_hex(3), "cycle_id": cycle_id, "letter": ch,
         "order_no": i, "total": len(phrase)}
        for i, ch in enumerate(phrase, start=1)
    ]
    random.shuffle(messages)
    return messages


# Set once a client has received a complete cycle; main() exits when it fires
done = asyncio.Event()


async def handler(websocket):
    """Send one scrambled cycle to a client, then signal the server to shut down."""
    client = websocket.remote_address
    print(f"Client connected: {client}; first message in {START_DELAY}s")
    try:
        await asyncio.sleep(START_DELAY)
        messages = build_cycle(PHRASE)
        print(f"Starting cycle {messages[0]['cycle_id']} for {client}")
        for msg in messages:
            await websocket.send(json.dumps(msg))
        print(f"Cycle {messages[0]['cycle_id']} complete for {client}")
        await websocket.close()
        done.set()
    except ConnectionClosed:
        print(f"Client disconnected mid-cycle: {client}")
    except Exception as e:
        print(f"Error with client {client}: {e}")


async def main():
    """Serve until one client has received a full cycle, then exit."""
    async with serve(handler, HOST, PORT):
        print(f"Puzzle server listening on ws://{HOST}:{PORT}")
        await done.wait()
    print("Round complete. Shutting down.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server stopped.")
