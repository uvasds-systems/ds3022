"""
Fake firehose: a WebSocket server that emits exactly RATE messages/sec (default 1000).

- One generator produces fake trade events at a fixed rate, independent of any client.
- Every message carries a global, monotonically increasing `seq`.
- The last BUFFER_SECONDS of messages are kept in a ring buffer, so clients can
  resume with  ws://host:8765/?cursor=<seq>  after a disconnect.
- A client that falls more than BUFFER_SECONDS behind is disconnected, the same way
  real firehoses treat slow readers. Note: the OS socket buffers (several MB on
  localhost) absorb ~30-40 s of backlog before the server itself sees lag; consumers
  should measure their own lag from `seq` / `ts`.

Run:  python main.py      then connect to ws://localhost:8765/
"""
import asyncio
import json
import os
import random
import signal
import time
from urllib.parse import parse_qs, urlparse

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

PORT = int(os.getenv("PORT", 8765))
RATE = int(os.getenv("RATE", 1000))                 # messages per second
BUFFER_SECONDS = int(os.getenv("BUFFER_SECONDS", 60))
SIZE = RATE * BUFFER_SECONDS                        # ring buffer capacity (messages)

buf: list[str | None] = [None] * SIZE               # ring buffer: seq -> buf[seq % SIZE]
head = -1                                           # seq of the newest message
new_data = asyncio.Event()                          # replaced and set after each batch
clients: dict[int, dict] = {}                       # id(ws) -> {"ws", "addr", "cursor"}

SYMBOLS = {"AAPL": 230.0, "MSFT": 420.0, "NVDA": 140.0, "AMZN": 200.0, "GOOG": 170.0,
           "META": 580.0, "TSLA": 250.0, "AMD": 150.0, "INTC": 22.0, "NFLX": 750.0}


def make_event(seq: int) -> str:
    """Build one fake trade event (random-walk price) as a JSON string."""
    sym = random.choice(list(SYMBOLS))
    SYMBOLS[sym] = round(max(1.0, SYMBOLS[sym] * (1 + random.gauss(0, 0.0005))), 2)
    return json.dumps({
        "seq": seq,
        "ts": time.time(),                          # emit time; consumers can compute end-to-end lag
        "symbol": sym,
        "price": SYMBOLS[sym],
        "qty": random.randint(1, 500),
        "side": random.choice(("buy", "sell")),
    })


async def generate() -> None:
    """Emit messages on a fixed schedule: total emitted = elapsed_seconds * RATE.

    Pacing uses absolute time rather than sleep durations, so sleep jitter never
    accumulates into drift; the long-run rate is exactly RATE.
    """
    global head, new_data
    t0 = time.monotonic()
    while True:
        due = int((time.monotonic() - t0) * RATE) - 1   # seq that should exist by now
        while head < due:
            head += 1
            buf[head % SIZE] = make_event(head)
        ev, new_data = new_data, asyncio.Event()        # wake every waiting client
        ev.set()
        for c in list(clients.values()):                # watchdog for slow consumers
            if c["cursor"] <= head - SIZE:
                drop(c)
        await asyncio.sleep(0.005)


def start_cursor(path: str) -> int:
    """Parse ?cursor=N from the request path; default is 'live' (next new message)."""
    try:
        cursor = int(parse_qs(urlparse(path).query)["cursor"][0])
    except (KeyError, ValueError, IndexError):
        return head + 1
    oldest = max(0, head - SIZE + 1)
    return min(max(cursor, oldest), head + 1)           # clamp into the replayable window


def drop(state: dict) -> None:
    """Disconnect a client whose next message has been overwritten in the ring buffer.

    A slow client's send() is blocked behind full socket buffers, so a polite close
    frame could never reach it; abort the TCP connection instead (client sees 1006).
    """
    print(f"[drop] {state['addr']} too slow: {head - state['cursor'] + 1} msgs behind")
    clients.pop(id(state["ws"]), None)                 # prevent repeat drops
    state["ws"].transport.abort()


async def handler(ws) -> None:
    """Stream messages to one client from its cursor; drop it if it falls out of the buffer."""
    state = {"ws": ws, "addr": ws.remote_address, "cursor": start_cursor(ws.request.path)}
    clients[id(ws)] = state
    print(f"[connect] {state['addr']} starting at seq {state['cursor']} (head {head})")
    try:
        while True:
            while state["cursor"] <= head:
                if state["cursor"] <= head - SIZE:      # message already overwritten
                    drop(state)
                    return
                # send() awaits when the socket's write buffer is full: a slow
                # reader stalls only its own loop, never the generator.
                await ws.send(buf[state["cursor"] % SIZE])
                state["cursor"] += 1
                if state["cursor"] % 500 == 0:
                    await asyncio.sleep(0)              # yield during large replays
            await new_data.wait()
    except ConnectionClosed:
        pass
    except Exception as e:
        print(f"[error] {state['addr']}: {e!r}")
    finally:
        clients.pop(id(ws), None)
        print(f"[disconnect] {state['addr']} at seq {state['cursor']}")


async def report(interval: int = 5) -> None:
    """Print measured emit rate and each client's lag every `interval` seconds."""
    last_head, last_t = head, time.monotonic()
    while True:
        await asyncio.sleep(interval)
        now = time.monotonic()
        rate = (head - last_head) / (now - last_t)
        last_head, last_t = head, now
        lags = ", ".join(f"{c['addr'][1]}:{head - c['cursor'] + 1}" for c in clients.values()) or "-"
        print(f"[stats] head={head} rate={rate:,.0f}/s clients={len(clients)} lag(msgs)={lags}")


async def main() -> None:
    stop = asyncio.get_running_loop().create_future()
    for sig in (signal.SIGINT, signal.SIGTERM):         # clean shutdown, incl. as Docker PID 1
        asyncio.get_running_loop().add_signal_handler(sig, stop.set_result, None)
    tasks = [asyncio.create_task(generate()), asyncio.create_task(report())]
    async with serve(handler, "0.0.0.0", PORT):
        print(f"Streaming {RATE} msg/s on ws://0.0.0.0:{PORT}/  (replay window {BUFFER_SECONDS}s)")
        await stop
    for t in tasks:
        t.cancel()


if __name__ == "__main__":
    asyncio.run(main())
