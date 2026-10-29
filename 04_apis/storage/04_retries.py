# 04 - Timeouts, retries, and rate limits.
# Some failures are temporary (timeouts, 5xx, 429) and worth retrying with backoff.
# Others are permanent (404, 401): retrying won't help, so fail fast.
# Try it: set TIMEOUT = 0.001 and watch the retries in the console and events.log.
import logging
import time

import httpx

USER = "schacon"
URL = "https://api.github.com/users/{user}/events/public"
TIMEOUT = 10.0      # seconds; with no timeout, a stalled request can hang forever
MAX_RETRIES = 4
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Log to a file AND the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("events.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)  # quiet httpx's per-request logs


def fetch_with_retries(url: str) -> httpx.Response:
    """GET a URL, retrying temporary failures with exponential backoff (1s, 2s, 4s...)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = httpx.get(url, timeout=TIMEOUT)
        except httpx.TransportError as e:
            # Network-level failure (DNS, refused connection, timeout): retryable
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: {e!r}")
        else:
            remaining = response.headers.get("X-RateLimit-Remaining")
            logger.info(f"Attempt {attempt}: HTTP {response.status_code}, rate limit remaining={remaining}")

            # Rate limit used up: the reset can be up to an hour away, so stop instead of sleeping
            if response.status_code in (403, 429) and remaining == "0":
                reset = int(response.headers.get("X-RateLimit-Reset", 0))
                logger.error(f"Rate limit exhausted; resets at {time.ctime(reset)}")
                response.raise_for_status()

            # Permanent errors raise immediately; success returns
            if response.status_code not in RETRYABLE_STATUS:
                response.raise_for_status()
                return response
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES}: retryable HTTP {response.status_code}")

        if attempt < MAX_RETRIES:
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Gave up on {url} after {MAX_RETRIES} attempts")


try:
    data = fetch_with_retries(URL.format(user=USER)).json()
    for item in data:
        print(item["repo"]["name"], " - ", item["type"])
    logger.info(f"Fetched {len(data)} events for {USER}")
except httpx.HTTPStatusError as e:
    logger.error(f"Permanent HTTP error for {USER}: {e}")
except Exception:
    # logger.exception records the full traceback, not just the message
    logger.exception(f"Unrecoverable failure fetching events for {USER}")
