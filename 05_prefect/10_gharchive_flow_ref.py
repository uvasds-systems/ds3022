import gzip
import json
from collections import Counter
import requests
from prefect import flow, task


BASE_URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"
OUT_FILE = "gharchive_counts.json"


@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    with requests.get(BASE_URL.format(hour=hour), stream=True, timeout=60) as r:
        r.raise_for_status()
        with gzip.open(r.raw) as f:
            counts = Counter(json.loads(line)["type"] for line in f)
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    return dict(sum(map(Counter, hourly_counts), Counter()).most_common())


# @task
# def save_counts(totals):
#     with open(OUT_FILE, "w") as f:
#         json.dump(totals, f, indent=2)
#     print(f"Saved {len(totals)} event types to {OUT_FILE}")


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    hourly = count_events_for_hour.map(hours).result()  # fan out, one task per hour
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)

if __name__ == "__main__":
    gharchive_flow()
