# 07 - End-to-end ingest: GitHub API -> raw JSON file -> DuckDB table.
# Combines 03-06. The raw response is saved to disk untouched before loading, so a
# failed load can be re-run without calling the API again (raw vs. staged, as in dbt).
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx

USER = "schacon"
URL = f"https://api.github.com/users/{USER}/events/public"
DATA_DIR = Path(__file__).parent / "data"
DB_PATH = DATA_DIR / "github.duckdb"
TIMEOUT, MAX_RETRIES, MAX_PAGES = 10.0, 4, 5
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


def get(client: httpx.Client, url: str, params: dict | None = None) -> httpx.Response:
    """GET with exponential-backoff retries on temporary failures (see 04_retries.py)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = client.get(url, params=params)
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
                return response
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: HTTP {response.status_code}")
        except httpx.TransportError as e:
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: {e!r}")
        if attempt < MAX_RETRIES:
            time.sleep(2 ** (attempt - 1))
    raise RuntimeError(f"Gave up on {url} after {MAX_RETRIES} attempts")


def extract() -> list[dict]:
    """Fetch all pages of events, using GH_PAT if set (see 05 and 06)."""
    headers = {"Accept": "application/vnd.github+json"}
    if token := os.environ.get("GH_PAT"):
        headers["Authorization"] = f"Bearer {token}"

    events, url, params = [], URL, {"per_page": 100}
    with httpx.Client(headers=headers, timeout=TIMEOUT) as client:
        for _ in range(MAX_PAGES):
            response = get(client, url, params)
            events.extend(response.json())
            url = response.links.get("next", {}).get("url")
            if not url:
                break
            params = None
    logger.info(f"Extracted {len(events)} events for {USER}")
    return events


def land(events: list[dict]) -> Path:
    """Write the raw API response to a timestamped JSON file, one per run."""
    raw_dir = DATA_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = raw_dir / f"events_{USER}_{ts}.json"
    path.write_text(json.dumps(events))
    logger.info(f"Landed raw file {path.name}")
    return path


def load(path: Path) -> None:
    """Load the raw file into DuckDB, replacing the table so re-runs give the same result."""
    with duckdb.connect(str(DB_PATH)) as con:
        con.execute(f"CREATE OR REPLACE TABLE raw_github_events AS SELECT * FROM read_json_auto('{path}')")
        count = con.execute("SELECT count(*) FROM raw_github_events").fetchone()[0]
    logger.info(f"Loaded {count} rows into {DB_PATH.name}:raw_github_events")


if __name__ == "__main__":
    try:
        load(land(extract()))
    except Exception:
        logger.exception("Pipeline failed")
        raise SystemExit(1)  # non-zero exit tells a scheduler or orchestrator the run failed
