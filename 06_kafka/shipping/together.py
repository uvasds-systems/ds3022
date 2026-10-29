#!/usr/bin/env python3

"""
Shipping Pipeline Launcher

Starts the producer, consumer, and Streamlit map as child processes, so the whole
pipeline runs from one terminal. Ctrl+C (or any one process exiting) stops all three.

Requires AISS_TOKEN in the environment (used by the producer).

Run with:  python 06_kafka/shipping/together.py
"""

import sys
import time
import logging
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
PY = sys.executable  # same interpreter/venv that launched this script

# Name -> command; the map is a Streamlit app, so it runs through "streamlit run"
COMMANDS = {
    "producer": [PY, "shipping-producer.py"],
    "consumer": [PY, "shipping-consumer.py"],
    "map": [PY, "-m", "streamlit", "run", "shipping-map.py", "--server.headless", "true"],
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(HERE / "together.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


# Launch each script from this folder so their log files land here too
def start_all():
    procs = {}
    for name, cmd in COMMANDS.items():
        try:
            procs[name] = subprocess.Popen(cmd, cwd=HERE)
            log.info(f"Started {name} (pid {procs[name].pid})")
        except OSError as e:
            log.error(f"Failed to start {name}: {e}")
            stop_all(procs)
            sys.exit(1)
        if name == "producer":
            time.sleep(3)  # let the producer create the topic before the consumer subscribes
    return procs


# Ask each child to stop, then force-kill any that don't exit within 10s
def stop_all(procs):
    for p in procs.values():
        if p.poll() is None:
            p.terminate()
    for name, p in procs.items():
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            log.warning(f"{name} did not exit; killing")
            p.kill()
        log.info(f"Stopped {name} (exit {p.returncode})")


if __name__ == "__main__":
    procs = start_all()
    log.info("Map at http://localhost:8501. Press Ctrl+C to stop all")
    try:
        # Watch the children; if any one exits, shut the rest down
        while True:
            for name, p in procs.items():
                if p.poll() is not None:
                    log.warning(f"{name} exited with code {p.returncode}; stopping the rest")
                    raise SystemExit
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        stop_all(procs)
