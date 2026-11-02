# Redis

## What is Redis?

[Redis](https://redis.io/) (REmote DIctionary Server) is an open-source, in-memory data store.
It is most often used as a **cache**, a fast **key-value database**, or a lightweight
**message broker**. Because data lives in RAM rather than on disk, reads and writes typically
take well under a millisecond.

## How it works

- Redis runs as a server process. Clients connect to it over TCP (default port **6379**) and
  send simple commands like `SET`, `GET`, and `KEYS`.
- All data is held in memory. Redis can optionally persist snapshots or an append-only log to
  disk so data survives a restart, but memory is the primary store.
- Commands are executed one at a time on a single thread, so each individual command is
  atomic: two clients can never half-write the same key.

## Key-value basics

Redis is essentially a very large, very fast dictionary.

- A **key** is a unique string, e.g. `user:1001:name`. A colon-delimited naming convention
  is common for grouping related keys.
- A **value** is the data stored at that key. The simplest type is a string, but Redis also
  supports lists, sets, hashes, sorted sets, and streams.
- You look data up **by key only**. There are no tables, joins, or SQL queries. If you don't
  know the key, you can't efficiently find the value.
- Setting a key that already exists **overwrites** it.
- Keys can be given an optional **expiration (TTL)**, after which Redis deletes them
  automatically. This is what makes Redis useful as a cache.

| Command | Purpose |
|---|---|
| `SET key value` | Store a value |
| `GET key` | Retrieve a value |
| `KEYS pattern` | List keys matching a pattern (e.g. `*`) |
| `DEL key` | Delete a key |
| `EXPIRE key seconds` | Set a time-to-live on a key |

## Your Redis instance

The Kafka cluster you started in `06_kafka/` with `docker compose` already includes a Redis
container named `redis`, listening on **localhost:6379**. Make sure that stack is running:

```bash
cd 06_kafka
docker compose up -d
docker ps | grep redis
```

You can also talk to Redis directly from the command line inside the container:

```bash
docker exec -it redis redis-cli
127.0.0.1:6379> SET greeting "hello"
127.0.0.1:6379> GET greeting
127.0.0.1:6379> KEYS *
```

## Python

The `redis` package is already in the project's environment. See `set-get.py` for the
basics: connecting, listing keys, setting a key, and fetching its value.

```bash
python 07_redis/set-get.py
```
