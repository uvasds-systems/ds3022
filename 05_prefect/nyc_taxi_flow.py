"""
Pulls all 2025 NYC Yellow Taxi trip data, compiles it into a local DuckDB
database, and reports total ride count + average fare/price for the year.

Source: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-{MM}.parquet
"""

from pathlib import Path

import duckdb
import requests
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

YEAR = 2025
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
DATA_DIR = Path("./nyc_taxi_data")
DB_PATH = Path("./nyc_taxi_2025.duckdb")
TABLE_NAME = "yellow_tripdata_2025"


@task(retries=3, retry_delay_seconds=10)
def download_month(year: int, month: int) -> Path | None:
    logger = get_run_logger()
    url = BASE_URL.format(year=year, month=month)
    dest = DATA_DIR / f"yellow_tripdata_{year}-{month:02d}.parquet"

    if dest.exists() and dest.stat().st_size > 0:
        logger.info(f"{dest.name} already downloaded ({dest.stat().st_size / 1e6:.1f} MB), skipping")
        return dest

    logger.info(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=60)

    if response.status_code == 404:
        logger.warning(f"{url} returned 404 (month not published yet?) — skipping")
        return None
    response.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_dest = dest.with_suffix(".parquet.part")
    total_bytes = 0
    # larger chunks = fewer Python-level iterations per file; network throughput is
    # the actual bottleneck here, not this loop, but no reason to leave it smaller
    with open(tmp_dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)
            total_bytes += len(chunk)
    tmp_dest.rename(dest)

    logger.info(f"Downloaded {dest.name} ({total_bytes / 1e6:.1f} MB)")
    return dest


@task
def compile_into_duckdb(parquet_paths: list[Path]) -> Path:
    logger = get_run_logger()
    valid_paths = [str(p) for p in parquet_paths if p is not None]

    if not valid_paths:
        raise RuntimeError("No parquet files were successfully downloaded — nothing to compile")

    logger.info(f"Compiling {len(valid_paths)} monthly files into DuckDB at {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))
    # duckdb defaults threads to CPU count already; set explicitly so the parallelism
    # here isn't left implicit
    con.execute("PRAGMA threads=8")
    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    # union_by_name handles any month-to-month schema drift (extra/renamed columns)
    # in the source parquet files, which the NYC TLC dataset is known to have.
    # read_parquet() over a file list already scans files in parallel internally.
    files_literal = "[" + ", ".join(f"'{p}'" for p in valid_paths) + "]"
    con.execute(
        f"CREATE TABLE {TABLE_NAME} AS "
        f"SELECT * FROM read_parquet({files_literal}, union_by_name=true)"
    )
    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    con.close()

    logger.info(f"Compiled {row_count:,} total rows into {TABLE_NAME}")
    return DB_PATH


@task
def compute_metrics(db_path: Path) -> dict:
    """
    Single pass over the table computing every aggregate we need at once —
    avoids scanning 47M+ rows twice for what were previously two separate tasks.
    """
    logger = get_run_logger()
    con = duckdb.connect(str(db_path))
    total_rides, avg_fare, avg_total_amount = con.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE total_amount > 0),
            AVG(fare_amount) FILTER (WHERE fare_amount > 0),
            AVG(total_amount) FILTER (WHERE total_amount > 0)
        FROM {TABLE_NAME}
        """
    ).fetchone()
    con.close()

    metrics = {
        "average_fare": avg_fare,
        "total_rides": total_rides,
        "average_price_per_ride": avg_total_amount,
    }
    logger.info(
        f"2025 metrics — total rides: {total_rides:,}, "
        f"average fare: ${avg_fare:,.2f}, average price per ride: ${avg_total_amount:,.2f}"
    )
    return metrics



# max_workers=12 guarantees all 12 monthly downloads actually run concurrently,
# rather than relying on whatever Prefect's default pool size happens to be.
@flow(name="nyc-yellow-taxi-2025-etl", task_runner=ThreadPoolTaskRunner(max_workers=12))
def nyc_yellow_taxi_2025_etl():
    logger = get_run_logger()
    logger.info(f"Starting NYC Yellow Taxi {YEAR} ETL")

    # 1. pull all months — all 12 download tasks are submitted immediately and run
    # concurrently (I/O-bound, so threads are the right tool here); .result() below
    # is what actually blocks until each one finishes.
    downloaded = download_month.map(year=YEAR, month=range(1, 13))
    parquet_paths = [f.result() for f in downloaded]

    # 2. compile into a local duckdb database
    db_path = compile_into_duckdb(parquet_paths)

    # 3 & 4. total rides + average fare + average price per ride, one table scan
    metrics = compute_metrics(db_path)

    print("\n=== NYC Yellow Taxi 2025 ETL — Final Report ===")
    print(f"Average fare_amount (metered fare only): ${metrics['average_fare']:,.2f}")
    print(f"Total rides in {YEAR}:                    {metrics['total_rides']:,}")
    print(f"Average price per ride (total_amount):    ${metrics['average_price_per_ride']:,.2f}")
    print("================================================\n")

    return metrics


if __name__ == "__main__":
    nyc_yellow_taxi_2025_etl()
