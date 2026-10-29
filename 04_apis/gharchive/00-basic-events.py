# 00 - Fetch one full month of GH Archive hourly event files.
# GH Archive publishes one gzipped, newline-delimited JSON file per hour:
#   https://data.gharchive.org/2025-01-{01..31}-{0..23}.json.gz
# Day is zero-padded, hour is NOT. 31 days x 24 hours = 744 files (expect tens of GB).
#
# Each function below maps to a future Prefect task; main() becomes the flow.

import logging
from pathlib import Path

import httpx


YEAR, MONTH, DAYS = 2025, 1, 31
URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
DATA_DIR = Path(__file__).parent / "data"
LOG_FILE = Path(__file__).parent / "gharchive.log"

logging.basicConfig(
  filename = LOG_FILE,
  level = logging.INFO,
  format = "%(asctime)s - %(levelname)s - %(message)s"
)


def build_urls(year, month, days):
  """Return every hourly archive URL for the month, in chronological order."""
  return [
    URL.format(year=year, month=month, day=day, hour=hour)
    for day in range(1, days + 1)
    for hour in range(24)
  ]


def download_hour(client, url):
  """Stream one hourly file to DATA_DIR. Skips files already on disk.
  Writes to a .part file first so an interrupted download is never mistaken for a complete one."""
  dest = DATA_DIR / url.rsplit("/", 1)[-1]
  if dest.exists():
    logging.info(f"Skipping {dest.name} (already downloaded)")
    return dest

  tmp = dest.with_suffix(".part")
  with client.stream("GET", url) as response:
    response.raise_for_status()
    with open(tmp, "wb") as f:
      for chunk in response.iter_bytes(chunk_size=1024 * 1024):
        f.write(chunk)
  tmp.rename(dest)
  logging.info(f"Downloaded {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
  return dest


def main():
  """Download all hours for the month; log and continue past individual failures."""
  DATA_DIR.mkdir(exist_ok=True)
  urls = build_urls(YEAR, MONTH, DAYS)
  failed = []

  with httpx.Client(timeout=60, follow_redirects=True) as client:
    for i, url in enumerate(urls, 1):
      try:
        download_hour(client, url)
        print(f"[{i}/{len(urls)}] {url}")
      except (httpx.HTTPError, OSError) as e:
        failed.append(url)
        logging.error(f"Failed {url}: {e}")
        print(f"[{i}/{len(urls)}] FAILED {url}: {e}")

  logging.info(f"Done: {len(urls) - len(failed)} succeeded, {len(failed)} failed")
  print(f"Done: {len(urls) - len(failed)} succeeded, {len(failed)} failed (see {LOG_FILE.name})")


if __name__ == "__main__":
  main()
