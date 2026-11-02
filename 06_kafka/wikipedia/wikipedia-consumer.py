from quixstreams import Application
import json
import redis

def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-edits"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                # get out the "type" key from the value
                change_type = value.get("type")
                
                # Only process if change_type is not None
                if change_type is not None:
                    # save these values to redis and use incr() to increment the value
                    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
                    redis_client.incr(change_type)
                    # print(f"Type count: {redis_client.get(change_type).decode('utf8')}")
                    redis_client.close()

                    # print on the screen
                    print(f"{offset} --> {change_type}")
                else:
                    print(f"{offset} {key} - No 'type' field in message")
                # print(f"{offset} {key} {value}")
                consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        redis_client = redis.Redis(host="localhost", port=6379, db=0)
        # for each key in redis, print the key and value
        for key in redis_client.keys():
            print(f"Type: {key.decode('utf8')} | Count: {redis_client.get(key).decode('utf8')}")
        redis_client.close()
        pass
