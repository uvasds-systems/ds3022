# 02 - Catching errors. raise_for_status() turns a 4xx/5xx response into an exception.
# USER is misspelled on purpose (404). httpx.HTTPError catches both HTTP-status
# errors and network errors; 04_retries.py treats them separately.

import httpx
import json

USER = "schaconz"
URL = "https://api.github.com/users/{user}/events/public"

try:
    response = httpx.get(URL.format(user=USER))
    response.raise_for_status()

    data = response.json()

    for item in data:
        print(item["repo"]["name"], " - ",item["type"])

except httpx.HTTPError as e:
    print(f"Error fetching events for {USER}: {e}")
