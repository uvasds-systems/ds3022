# 05 - Pagination.
# APIs rarely return everything in one response. GitHub sends 30 items per page by
# default and puts the next page's URL in the `Link` response header, which httpx
# parses into response.links.
import logging

import httpx

USER = "schacon"
URL = "https://api.github.com/users/{user}/events/public"
PER_PAGE = 100
MAX_PAGES = 5  # safety limit so a bug can't page forever

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("events.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


def fetch_all(url: str) -> list[dict]:
    """Follow `next` links until there are no more pages (or MAX_PAGES is reached)."""
    events, params = [], {"per_page": PER_PAGE}
    # A Client reuses one connection across requests, which is faster than repeated httpx.get()
    with httpx.Client(timeout=10.0) as client:
        for page in range(1, MAX_PAGES + 1):
            response = client.get(url, params=params)
            response.raise_for_status()
            batch = response.json()
            events.extend(batch)
            logger.info(f"Page {page}: {len(batch)} events (running total {len(events)})")

            url = response.links.get("next", {}).get("url")
            if not url:
                break
            params = None  # the `next` URL already includes its query string
    return events


try:
    events = fetch_all(URL.format(user=USER))
    logger.info(f"Fetched {len(events)} events for {USER} across all pages")
except httpx.HTTPStatusError as e:
    logger.error(f"HTTP error for {USER}: {e}")
except httpx.RequestError as e:
    logger.error(f"Network error for {USER}: {e!r}")
