# `redis-cli`

Basic `redis` operations using the client.

Connect to the Redis container running in your Kafka compose stack:

Get all keys (fine for learning; avoid KEYS * on large production DBs)

    keys *


Set a key. Overwrites any existing value at this key.

    set course:name "DS3022 Data Engineering"

Set a key that expires after 30 seconds

    set course:temp "gone soon" ex 30

Fetch a value by key. Returns `(nil)` if the key does not exist.

    get course:name
    get no:such:key

print("course:name =", r.get("course:name"))
print("missing     =", r.get("no:such:key"))

View keys matching a pattern

    keys "course:*"

Remaining time-to-live (seconds) on the expiring key

    ttl "course:temp"

Set an integer counter

    set my_counter 0

Add +1 to the counter

    incr my_counter
    incr my_counter
    incr my_counter

Fetch the value of the counter

    get my_counter