#!/usr/bin/env python3

"""
Shipping Stream Consumer

Reads AIS messages from the "shipping" Kafka topic and keeps two Redis keys per vessel:
  ship:<mmsi>   hash of the latest state (position, speed, heading, name, type, destination)
  track:<mmsi>  sorted set of "lon,lat" points scored by AIS timestamp, trimmed to TRACK_SECONDS

A point joins the track only after the ship has moved MIN_MOVE_M from the last track point,
which keeps 24h trails small enough to redraw quickly.

The Streamlit map (shipping-map.py) reads these to draw each ship's trail.

Kafka: KAFKA_BROKER, plus KAFKA_USERNAME / KAFKA_PASSWORD to enable SASL (k8s cluster).
"""

import os
import math
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
import redis
from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
TRACK_SECONDS = 24 * 60 * 60  # trail length kept per ship; ships silent this long expire
MAX_KNOTS = 50               # implied speed above this is a GPS jump, not real movement
MIN_MOVE_M = 500             # distance from the last track point before a new one is added

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    # Rotate at 10 MB (2 backups) so a long-running pod does not fill its disk
    handlers=[RotatingFileHandler("shipping-consumer.log", maxBytes=10_000_000, backupCount=2),
              logging.StreamHandler()],
)
log = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


# Plain broker list locally; SASL_PLAINTEXT/PLAIN when KAFKA_USERNAME is set (the k8s cluster)
def kafka_connection():
    user = os.getenv("KAFKA_USERNAME")
    if not user:
        return KAFKA_BROKER
    password = os.getenv("KAFKA_PASSWORD")
    if not password:
        log.error("KAFKA_USERNAME is set but KAFKA_PASSWORD is not")
        raise SystemExit(1)
    return ConnectionConfig(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username=user,
        sasl_password=password,
    )


app = Application(
    broker_address=kafka_connection(),
    consumer_group="shipping-redis",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic("shipping", value_deserializer="json")


# Great-circle distance in meters between two lat/lon points
def haversine_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6_371_000 * math.asin(math.sqrt(a))


# "2026-10-26 17:00:26.020311 +0000 UTC" -> unix seconds (sub-second precision not needed)
def parse_time(s):
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


# Static data (name, type, destination) arrives every few minutes; merge it into the ship hash
def handle_static(mmsi, body):
    r.hset(f"ship:{mmsi}", mapping={
        "type": body.get("Type", 0),
        "destination": body.get("Destination", "").strip(),
        "callsign": body.get("CallSign", "").strip(),
    })
    r.expire(f"ship:{mmsi}", TRACK_SECONDS)


# Update the ship's latest state and append to its track, filtering jumps and jitter
def handle_position(mmsi, meta, body):
    lat, lon = meta["latitude"], meta["longitude"]
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):  # AIS uses 91/181 for "not available"
        return
    ts = parse_time(meta["time_utc"])
    ship_key, track_key = f"ship:{mmsi}", f"track:{mmsi}"

    # Compare with the previous report to reject out-of-order reports and GPS jumps
    prev = r.hmget(ship_key, "lat", "lon", "time")
    if prev[0] is not None:
        plat, plon, pts = map(float, prev)
        dist, dt = haversine_m(plat, plon, lat, lon), ts - pts
        if dt < 0:
            return  # out-of-order report; keep the newer state
        if dt > 0 and dist / dt * 1.944 > MAX_KNOTS:
            log.warning(f"{mmsi} jump of {dist:.0f} m in {dt:.0f}s; skipped")
            return

    # Compare with the last track point (not the last report) to decide whether to extend the track
    last = r.zrange(track_key, -1, -1)
    if last:
        llon, llat = map(float, last[0].split(","))
        add_point = haversine_m(llat, llon, lat, lon) >= MIN_MOVE_M
    else:
        add_point = True

    heading = body.get("TrueHeading", 511)
    pipe = r.pipeline()
    pipe.hset(ship_key, mapping={
        "name": meta.get("ShipName", "").strip() or str(mmsi),
        "lat": lat, "lon": lon, "time": ts,
        "sog": body.get("Sog", 0),
        "cog": body.get("Cog", 0),
        "heading": heading if heading != 511 else body.get("Cog", 0),  # 511 = heading unavailable
    })
    if add_point:
        pipe.zadd(track_key, {f"{lon:.5f},{lat:.5f}": ts})
    pipe.zremrangebyscore(track_key, "-inf", ts - TRACK_SECONDS)
    pipe.expire(ship_key, TRACK_SECONDS)
    pipe.expire(track_key, TRACK_SECONDS)
    pipe.execute()


# Route each message by type; one bad message must not stop the consumer
def to_redis(event):
    try:
        kind, meta = event["MessageType"], event["MetaData"]
        body = event["Message"][kind]
        if kind == "ShipStaticData":
            handle_static(meta["MMSI"], body)
        else:
            handle_position(meta["MMSI"], meta, body)
    except (KeyError, ValueError, TypeError) as e:
        log.warning(f"Skipping malformed message: {e!r}")
    except redis.RedisError as e:
        log.error(f"Redis write failed: {e}")


if __name__ == "__main__":
    sdf = app.dataframe(topic)
    sdf = sdf.update(to_redis)
    log.info(f"Consuming '{topic.name}' -> Redis at {REDIS_HOST}:6379. Press Ctrl+C to stop")
    app.run()
