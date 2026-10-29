"""
Downloads NYC Yellow Taxi monthly parquet files for a range of years and uploads
each one to s3://uvasds-data/taxi/ with a public-read ACL.

Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY}-{MM}.parquet

Usage:
    python upload_taxi_data.py --start 2015 --end 2024
"""

import argparse
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
BUCKET = "uvasds-data"
PREFIX = "taxi"
PUBLIC_URL = f"https://{BUCKET}.s3.amazonaws.com/{PREFIX}/{{name}}"
DATA_DIR = Path("./nyc_taxi_data")

# boto3 clients are thread-safe, so one shared client serves all upload threads
s3 = boto3.client("s3")


def s3_size(key: str) -> int | None:
    """Return the object's size in bytes, or None if it doesn't exist in S3."""
    try:
        return s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


@task(retries=3, retry_delay_seconds=10)
def transfer_month(year: int, month: int) -> tuple[str, int] | None:
    """Download one month, upload it public-read to S3, verify, and remove the local copy."""
    logger = get_run_logger()
    name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    key = f"{PREFIX}/{name}"

    # skip files already in the bucket so reruns don't re-download everything
    existing = s3_size(key)
    if existing:
        print(f"{key} already in S3 ({existing / 1e6:.1f} MB), skipping")
        return key, existing

    # stream the download to a .part file so a failed transfer never looks complete
    url = BASE_URL.format(year=year, month=month)
    response = requests.get(url, stream=True, timeout=60)
    if response.status_code in (403, 404):
        logger.warning(f"{url} returned {response.status_code} (not published?), skipping")
        return None
    response.raise_for_status()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / name
    tmp = dest.with_suffix(".parquet.part")
    with open(tmp, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)
    tmp.rename(dest)
    local_size = dest.stat().st_size

    # upload_file handles multipart automatically for large files
    s3.upload_file(str(dest), BUCKET, key, ExtraArgs={"ACL": "public-read"})

    # verify: S3 size matches local size, and the object is readable anonymously
    remote_size = s3_size(key)
    if remote_size != local_size:
        raise RuntimeError(f"{key}: size mismatch (local {local_size}, S3 {remote_size})")
    public = requests.head(PUBLIC_URL.format(name=name), timeout=30, allow_redirects=True)
    if public.status_code != 200:
        raise RuntimeError(f"{key}: uploaded but not publicly readable (HTTP {public.status_code})")

    dest.unlink()
    print(f"Uploaded + verified s3://{BUCKET}/{key} ({remote_size / 1e6:.1f} MB, public)")
    return key, remote_size


@flow(name="nyc-yellow-taxi-s3-upload", log_prints=True,
      task_runner=ThreadPoolTaskRunner(max_workers=12))
def upload_taxi_data(start: int, end: int):
    """Transfer every month in [start, end] concurrently, then print a summary."""
    years = [y for y in range(start, end + 1) for _ in range(12)]
    months = list(range(1, 13)) * (end - start + 1)
    results = [f.result() for f in transfer_month.map(year=years, month=months)]

    uploaded = [r for r in results if r]
    total = sum(size for _, size in uploaded)
    print(f"\n=== {len(uploaded)}/{len(results)} files in s3://{BUCKET}/{PREFIX}/ "
          f"({total / 1e9:.2f} GB total) ===")
    return uploaded


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=int, required=True, help="first year (inclusive)")
    parser.add_argument("--end", type=int, required=True, help="last year (inclusive)")
    args = parser.parse_args()
    if args.start > args.end:
        parser.error("--start must be <= --end")
    upload_taxi_data(args.start, args.end)
