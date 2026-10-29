# 06 - Authenticating with a token.
# Unauthenticated GitHub requests are limited to 60/hour; a personal access token
# raises that to 5,000. Never hardcode a token: read it from the environment
# (see 01_data/env_vars.py), and never write it to a log.
#
#   export GH_PAT=github_pat_...   # https://github.com/settings/tokens (no scopes needed)
import logging
import os

import httpx

RATE_URL = "https://api.github.com/rate_limit"  # checking this does not count against the limit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("events.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Send the token only if it is set; otherwise fall back to anonymous requests
token = os.environ.get("GH_PAT")
headers = {"Accept": "application/vnd.github+json"}
if token:
    headers["Authorization"] = f"Bearer {token}"
else:
    logger.warning("GH_PAT not set; making unauthenticated requests")

try:
    response = httpx.get(RATE_URL, headers=headers, timeout=10.0)
    response.raise_for_status()
    core = response.json()["resources"]["core"]
    logger.info(f"authenticated={bool(token)} limit={core['limit']} remaining={core['remaining']}")
except httpx.HTTPStatusError as e:
    # 401 means the request reached GitHub but the credentials were rejected
    if e.response.status_code == 401:
        logger.error("Token rejected (401): check that GH_PAT is valid and not expired")
    else:
        logger.error(f"HTTP error: {e}")
except httpx.RequestError as e:
    logger.error(f"Network error: {e!r}")
