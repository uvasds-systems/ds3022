#!/usr/bin/env python3

"""
Shipping Stream Producer

Connects to the AISStream.io websocket, subscribes to vessel position reports
inside a box around the Chesapeake Bay, and produces each message to the
"shipping" Kafka topic keyed by the vessel's MMSI.

Keying by MMSI sends every ping from one ship to the same partition, so a
consumer reads each ship's track in order.

Requires AISS_TOKEN in the environment.

Kafka: KAFKA_BROKER, plus KAFKA_USERNAME / KAFKA_PASSWORD to enable SASL (k8s cluster).
"""

import os
import json
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import websockets
from quixstreams import Application
from quixstreams.models import TopicConfig
from quixstreams.kafka.configuration import ConnectionConfig

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
AIS_URI = "wss://stream.aisstream.io/v0/stream"

# [[lat, lon], [lat, lon]] corners: Cape Henry / Norfolk approaches up to the Susquehanna,
# Potomac fall line east to the Atlantic side of the Delmarva peninsula
CHESAPEAKE_BOX = [[36.6, -77.6], [39.8, -75.2]]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    # Rotate at 10 MB (2 backups) so a long-running pod does not fill its disk
    handlers=[RotatingFileHandler("shipping-producer.log", maxBytes=10_000_000, backupCount=2),
              logging.StreamHandler()],
)
log = logging.getLogger(__name__)


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
    producer_extra_config={"broker.address.family": "v4"},
)
# Topic settings apply only when the topic is first created
topic = app.topic(
    name="shipping",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=3,
        replication_factor=3,
        extra_config={
            "retention.ms": str(24 * 60 * 60 * 1000),  # keep 24h of tracks
            "min.insync.replicas": "2",
        },
    ),
)


# Produce one AIS message to Kafka, keyed by MMSI
def process(payload, producer):
    try:
        data = json.loads(payload)
        if "error" in data:
            log.error(f"AISStream error: {data['error']}")
            return
        meta = data["MetaData"]
        msg = topic.serialize(key=str(meta["MMSI"]), value=data)
        producer.produce(topic=topic.name, key=msg.key, value=msg.value)
        log.info(f"{data['MessageType']:15} | {meta['MMSI']} | {meta['ShipName'].strip():20} "
                 f"| {meta['latitude']:.4f}, {meta['longitude']:.4f}")
    except (json.JSONDecodeError, KeyError) as e:
        log.warning(f"Skipping malformed message: {e}")
    except Exception as e:
        log.error(f"Error publishing to Kafka: {e}")


# Subscribe and stream until the connection drops
async def stream(producer):
    async with websockets.connect(AIS_URI, compression="deflate") as ws:
        # Subscription must be sent within 3 seconds of connecting
        await ws.send(json.dumps({
            "APIKey": os.environ["AISS_TOKEN"],
            "BoundingBoxes": [CHESAPEAKE_BOX],
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        }))
        log.info(f"Subscribed to {AIS_URI}, box {CHESAPEAKE_BOX}")
        async for payload in ws:
            process(payload, producer)


# Reconnect with exponential backoff; AISStream closes idle or overloaded connections
async def main():
    log.info(f"Producing to topic '{topic.name}' on {KAFKA_BROKER}. Press Ctrl+C to stop")
    delay = 1
    with app.get_producer() as producer:
        while True:
            try:
                await stream(producer)
                delay = 1
            except (websockets.ConnectionClosed, OSError) as e:
                log.warning(f"Connection lost ({e}); reconnecting in {delay}s")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopping stream")
