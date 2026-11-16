# Bluesky Producer: Resilience Notes

How `bluesky-producer-full-dataframe.py` handles failures, and the open design decisions.

## Pipeline

```
JetstreamSource  -->  bluesky-jetstream-raw  -->  StreamingDataFrame  -->  bluesky-events
(websocket ingest)    (all events, 1h TTL)        (filter + count)         (commit events, 3h TTL)
```

- **`JetstreamSource`** is a quixstreams `StatefulSource`. `app.run()` starts it in a subprocess, where it reads Jetstream and produces every event to `bluesky-jetstream-raw`.
- **The StreamingDataFrame** consumes the raw topic (consumer group `bluesky-producer-sdf`), drops non-commit events, counts collections, and writes the rest to `bluesky-events`. Quix commits its consumer offsets every 5s.

## What the producer does

- **Producer settings:** idempotence on (quixstreams enables it by default, which also means `acks=all`: every replica confirms a write). Also: lz4 compression, 20ms / 1MB batching, 60s limit on retrying a message (`message.timeout.ms`). The same settings apply to the source's producer and the SDF's producer.
- **Keys and timestamps:** the source keys messages by DID, so each user's events stay in order on one partition. The Kafka timestamp is the event time from Jetstream's `time_us`, not the time it was received. `to_topic()` carries both the key and the timestamp through to `bluesky-events`.
- **Resume point:** the Jetstream cursor is stored in the source's state store, not in Redis. Every 5s the source calls `flush()`, which waits until Kafka confirms all buffered messages and only then commits the cursor. The saved cursor therefore never runs ahead of what Kafka actually holds.
- **Where the cursor lives:** a local RocksDB store under `state/bluesky-producer-sdf/` (relative to the directory the script is run from), backed by the compacted changelog topic `changelog__bluesky-producer-sdf--source-jetstream`. If `state/` is deleted, Quix rebuilds it from the changelog on startup.
- **Jetstream drops:** fail over to the next of Bluesky's four public Jetstream hosts (`jetstream1`/`jetstream2` × `us-east`/`us-west`), resuming from the cursor minus a 2s rewind. The cursor is a timestamp, so it works on any host. Failover is immediate; exponential backoff (up to 60s) applies only after all four hosts have failed in a row.
- **Kafka slow or down:** the producer retries internally. If a message still fails, or the buffer stays full, the source raises an exception, the app stops, and the script exits with status 1. Because the cursor is only saved after a successful flush, restarting replays Jetstream from the last confirmed point. Nothing is lost as long as the restart happens while Jetstream still holds the events.
- **Exit:** Ctrl-C and SIGTERM are both handled by Quix. The source stops reading, flushes, and saves the cursor; the SDF commits its offsets.
- **Start from live:** set `START_LIVE=1` to ignore the saved cursor.
- **Topic settings:** applied only when a topic is created; an existing topic keeps its settings.

| Topic | Partitions | Replicas | Retention |
|---|---|---|---|
| `bluesky-jetstream-raw` | 3 | 3 | 1 hour |
| `bluesky-events` | 3 | 3 | 3 hours |
| `changelog__...source-jetstream` | 1 | 1 | compacted |

  Change retention on an existing topic with `rpk topic alter-config bluesky-events --set retention.ms=10800000`. Because the Kafka timestamp is the event time, replayed events that are already near the retention limit expire within minutes of being written.

## Open decisions

1. **Duplicates.** Delivery is at-least-once, and duplicates can come from two places:
   - **Source:** the 2s reconnect rewind, and a replay of up to 5s of events after a crash (anything produced since the last cursor save).
   - **SDF:** after a crash, up to 5s of raw events are reprocessed (anything since the last offset commit).

   Current position: acceptable, and a useful teaching point. Consumers that need to drop duplicates can use `did` + `commit.rkey` + `commit.rev` as the identifier.

2. **Writing every event twice.** Each event goes to Kafka twice: once raw, once filtered. That roughly doubles broker writes and storage, the cost of putting a StreamingDataFrame in the pipeline. The raw topic's 1-hour retention limits the storage cost. The alternative is to filter in the source and skip the SDF, which is what `bluesky-producer-full.py` does.

3. **Restarts.** *Resolved:* see "Resume point" above. Open question: a very old saved cursor will replay everything Jetstream still holds (potentially hours); see improvement 3 below.

4. **Long Kafka outages.** *Resolved, differently from the previous version:* there is no health-check loop. The app exits on the first delivery failure (after the producer's own retries), and the saved cursor makes the restart safe. Improvement 4 would make the restart automatic.

## Proposed improvements

Not yet implemented, ordered by value within each group.

### Resilience

1. **Replicate the changelog topic.** Quix created it with replication factor 1. If the broker holding it is down and `state/` is missing, the cursor can't be recovered. Redpanda can raise the replication factor in place: `rpk topic alter-config changelog__bluesky-producer-sdf--source-jetstream --set replication.factor=3`.
2. **Detect a stalled stream.** The source uses a synchronous `create_connection()`, which has no automatic pings. A connection that stays open but sends nothing just times out on `recv()` every 5s, forever. Fix: count consecutive timeouts and reconnect after about 30s with no messages.
3. **Ignore stale cursors.** A saved cursor older than the topic's 3-hour retention replays events Kafka deletes almost immediately. Start from the live stream when the saved cursor is older than `TOPIC_RETENTION_MS`.
4. **Restart automatically.** The script exits with status 1 when Kafka stays down. Running it as a `docker compose` service with `restart: on-failure` would restart it and resume from the saved cursor without intervention.
5. **Fail faster when Kafka is down.** `Source.produce()` defaults to a 30s poll × 10 retries when the buffer is full, so the source can block for up to 5 minutes before giving up. Passing `poll_timeout=2` and `buffer_error_max_tries=3` would surface the failure sooner.

### Performance

6. **Avoid re-encoding.** Each event is parsed in the source and re-encoded by `serialize()`, then parsed and re-encoded again by the SDF. Producing the original websocket bytes from the source removes one encode per message. Matters most during backfill (several thousand msg/sec in testing).
7. **Use `orjson` for parsing.** 2–5× faster than `json.loads`; one `pipenv install`.
8. **Add Kafka message headers.** Put `collection` and `operation` in headers so consumers can filter without parsing the JSON body.
9. **Narrow `wantedCollections`.** Likes are about 60–70% of the volume. If the exercises only use posts, follows and similar, filtering at Jetstream cuts bandwidth, CPU and Kafka storage.
10. **Turn on Jetstream compression.** `compress=true` roughly halves bandwidth but requires zstd and Jetstream's custom dictionary file. Skip unless bandwidth is a problem.

### Worth considering now

- **Exactly-once for the SDF stage.** `Application(processing_guarantee="exactly-once")` makes the raw → `bluesky-events` step transactional, which removes the SDF's duplicates. The source's rewind and replay duplicates remain. It is a one-line change, but it adds some latency and needs testing.

### Not worth doing here

- **`min.insync.replicas`.** Redpanda already requires a majority of replicas to confirm writes with `acks=all`.

## Tested / untested

- **Tested:**
  - Normal streaming.
  - Ctrl-C shutdown with the final cursor save.
  - Restart resuming from the saved cursor ("Loaded saved cursor ... from source state").
- **Untested:**
  - The Kafka-down path. To test: start the producer, `docker compose stop` the Redpanda brokers for longer than 60s, and confirm the app exits with status 1. Restart the brokers and the script, and confirm it resumes from the saved cursor.
  - Rebuilding state from the changelog. To test: delete `state/` between runs.
