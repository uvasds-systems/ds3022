# 01 - Load GH Archive hourly event files straight into DuckDB (no .gz files saved to disk).
# DuckDB reads each https://...json.gz URL directly, decompresses it, and parses the
# newline-delimited JSON in one step. Nested actor/repo/org objects are flattened into columns.
# The event-specific `payload` (whose shape differs per event type, and is most of the data)
# is skipped unless INCLUDE_PAYLOAD is True; the column always exists so the schema is stable.
#
# Each function below maps to a future Prefect task; main() becomes the flow.

import logging
from pathlib import Path

import duckdb


YEAR, MONTH = 2025, 1
START_DAY = 1   # first day of the month to load
NUM_DAYS = 31    # proof of concept: first 24 hours. Raise (up to 31) to load more.
URL = "https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"
DB_FILE = Path(__file__).parent / "gharchive.duckdb"
TABLE = "events"
INCLUDE_PAYLOAD = False  # True stores raw payload JSON (~30x larger database)
LOG_FILE = Path(__file__).parent / "gharchive.log"

logging.basicConfig(
  filename = LOG_FILE,
  level = logging.INFO,
  format = "%(asctime)s - %(levelname)s - %(message)s"
)


def build_urls(year, month, start_day, num_days):
  """Return every hourly archive URL for the requested days, in chronological order."""
  return [
    URL.format(year=year, month=month, day=day, hour=hour)
    for day in range(start_day, start_day + num_days)
    for hour in range(24)
  ]


def create_table(con):
  """Create the events table once. source_file records which hourly file each row came from."""
  con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
      id                  VARCHAR,
      type                VARCHAR,
      public              BOOLEAN,
      created_at          TIMESTAMPTZ,
      actor_id            BIGINT,
      actor_login         VARCHAR,
      actor_display_login VARCHAR,
      actor_gravatar_id   VARCHAR,
      actor_url           VARCHAR,
      actor_avatar_url    VARCHAR,
      repo_id             BIGINT,
      repo_name           VARCHAR,
      repo_url            VARCHAR,
      org_id              BIGINT,
      org_login           VARCHAR,
      org_gravatar_id     VARCHAR,
      org_url             VARCHAR,
      org_avatar_url      VARCHAR,
      payload             JSON,
      source_file         VARCHAR
    )
  """)


def load_hour(con, url):
  """Fetch, unpack, and insert one hourly file. Skips files already loaded.
  Runs in a transaction so a failed load never leaves a partial hour in the table."""
  if con.execute(f"SELECT 1 FROM {TABLE} WHERE source_file = ? LIMIT 1", [url]).fetchone():
    logging.info(f"Skipping {url} (already loaded)")
    return 0

  # Explicit column types stop DuckDB from inferring a different schema for every file.
  # Keys not listed (e.g. payload when disabled) are never parsed.
  payload_col = "payload: 'JSON'," if INCLUDE_PAYLOAD else ""
  payload_sel = "payload" if INCLUDE_PAYLOAD else "NULL"
  con.begin()
  try:
    con.execute(f"""
      INSERT INTO {TABLE}
      SELECT id, type, public, created_at,
             actor.id, actor.login, actor.display_login, actor.gravatar_id, actor.url, actor.avatar_url,
             repo.id, repo.name, repo.url,
             org.id, org.login, org.gravatar_id, org.url, org.avatar_url,
             {payload_sel}, ?
      FROM read_json(?,
        format = 'newline_delimited',
        compression = 'gzip',
        columns = {{
          id: 'VARCHAR', type: 'VARCHAR', public: 'BOOLEAN', created_at: 'TIMESTAMPTZ', {payload_col}
          actor: 'STRUCT(id BIGINT, login VARCHAR, display_login VARCHAR, gravatar_id VARCHAR, url VARCHAR, avatar_url VARCHAR)',
          repo: 'STRUCT(id BIGINT, name VARCHAR, url VARCHAR)',
          org: 'STRUCT(id BIGINT, login VARCHAR, gravatar_id VARCHAR, url VARCHAR, avatar_url VARCHAR)'
        }}
      )
    """, [url, url])
    rows = con.execute(f"SELECT count(*) FROM {TABLE} WHERE source_file = ?", [url]).fetchone()[0]
    con.commit()
  except duckdb.Error:
    con.rollback()
    raise
  logging.info(f"Loaded {rows:,} events from {url}")
  return rows


def main():
  """Load all requested hours; log and continue past individual failures."""
  urls = build_urls(YEAR, MONTH, START_DAY, NUM_DAYS)
  failed = []

  try:
    with duckdb.connect(DB_FILE) as con:
      create_table(con)
      for i, url in enumerate(urls, 1):
        try:
          rows = load_hour(con, url)
          print(f"[{i}/{len(urls)}] {rows:,} rows  {url}")
        except duckdb.Error as e:
          failed.append(url)
          logging.error(f"Failed {url}: {e}")
          print(f"[{i}/{len(urls)}] FAILED {url}: {e}")

      # Quick sanity check: what landed, by event type
      total = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
      print(f"\n{total:,} rows in {DB_FILE.name}:{TABLE}")
      con.sql(f"SELECT type, count(*) AS n FROM {TABLE} GROUP BY type ORDER BY n DESC").show()
  except duckdb.Error as e:
    logging.error(f"Database error: {e}")
    print(e)

  logging.info(f"Done: {len(urls) - len(failed)} succeeded, {len(failed)} failed")
  print(f"Done: {len(urls) - len(failed)} succeeded, {len(failed)} failed (see {LOG_FILE.name})")


if __name__ == "__main__":
  main()
