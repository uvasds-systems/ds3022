# Ingest: APIs, Exceptions, and Logging

Most pipelines start by pulling data from somewhere you don't control. Networks drop,
APIs change, rate limits run out. This module builds a small GitHub API client one step
at a time, adding the error handling and logging that make ingestion reliable.

Run each script from this folder, in order:

```bash
cd 04_ingest
pipenv run python 01_plain.py      # or: uv run python 01_plain.py
```

| Script | Concept |
|---|---|
| `01_plain.py` | The happy path. No error handling. |
| `02_errors.py` | `try`/`except`, `raise_for_status()` |
| `03_logging.py` | Logging to a file; library loggers (httpx) |
| `04_retries.py` | Timeouts, exponential backoff, rate limits; retryable vs. permanent errors |
| `05_pagination.py` | Following `Link` headers across pages; `httpx.Client` |
| `06_auth.py` | Tokens from environment variables; 401 vs. 404 |
| `07_pipeline.py` | All of the above: API → raw JSON in `data/raw/` → DuckDB table |

Logs are written to `events.log` (01–06) and `pipeline.log` (07). Log files and `data/` are git-ignored.

## Key ideas

- **Two kinds of failure.** `httpx.HTTPStatusError`: the server answered with a 4xx/5xx.
  `httpx.RequestError`: no usable answer came back (DNS failure, refused connection, timeout).
  They need different handling.
- **Retry temporary errors, not permanent ones.** A 503 or a timeout may succeed on the next
  try. A 404 or 401 won't.
- **Always set a timeout.** Without one, a stalled request can hang your job indefinitely.
- **Schema drift is an exception too.** A `KeyError` on `item["repo"]["name"]` means the API
  changed the shape of its data. This is the same problem as the schemas in `01_data`.
- **Log what you'll need at 3 a.m.** Use `logger.exception()` inside `except` to capture the
  traceback. Never log secrets.
- **Land raw data first.** Save the response untouched, then load it. You can re-run the load
  without calling the API again.

## GitHub token (optional, for 06–07)

Unauthenticated requests are limited to 60/hour. Create a fine-grained token at
<https://github.com/settings/tokens> (no permissions needed), then:

```bash
export GH_PAT=github_pat_...
```

## Exercise: break it on purpose

Trigger each failure below, then diagnose it **using only the log file**:

1. Misspell `USER` (404)
2. Turn off Wi-Fi (network error)
3. Set `TIMEOUT = 0.001` in `04_retries.py` (timeout and retries)
4. Set `GH_PAT` to garbage and run `06_auth.py` (401)
5. Run `01_plain.py` repeatedly without a token until you hit the rate limit (403). What do the
   `X-RateLimit-*` headers say?

For each one, answer: should the pipeline retry, or stop and alert someone?

## Going further

- [`tenacity`](https://tenacity.readthedocs.io/) is the standard library for production retry logic
  (`pipenv install tenacity`). Rewrite `fetch_with_retries` using its `@retry` decorator.
- Query the landed data: `duckdb data/github.duckdb "SELECT type, count(*) FROM raw_github_events GROUP BY 1"`.
