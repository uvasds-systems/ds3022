import redis

# Connect to the Redis container from 06_kafka/docker-compose.yml.
# decode_responses=True returns str instead of raw bytes.
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

# Confirm the server is reachable
r.ping()

# View all existing keys (fine for learning; avoid KEYS * on large production DBs)
print("Keys before:", r.keys("*"))

# Set a key. Overwrites any existing value at this key.
r.set("course:name", "DS3022 Data Engineering")

# Set a key that expires after 60 seconds
r.set("course:temp", "gone soon", ex=60)

# Fetch a value by key. Returns None if the key does not exist.
print("course:name =", r.get("course:name"))
print("missing     =", r.get("no:such:key"))

# View keys matching a pattern
print("Keys after:", r.keys("course:*"))

# Remaining time-to-live (seconds) on the expiring key
print("course:temp TTL:", r.ttl("course:temp"))

# Set an integer counter
r.set("my_counter", 0)

# Add +1 to the counter. INCR is atomic and returns the new value.
r.incr("my_counter")
r.incr("my_counter")
r.incr("my_counter")

# Fetch the value of the counter (returned as a str, e.g. "3")
print("my_counter =", r.get("my_counter"))
