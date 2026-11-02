import redis

# Connect to the Redis container from 06_kafka/docker-compose.yml.
r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
r.ping()

KEY = "cville:coffee"

# Start fresh so re-runs don't accumulate stale members
r.delete(KEY)

# Example coffee shops around Charlottesville, VA (approximate coordinates).
# Note: Redis geo commands take LONGITUDE first, then latitude.
shops = [
    (-78.5003, 38.0345, "Grit Coffee - The Corner"),
    (-78.4808, 38.0306, "Mudhouse - Downtown Mall"),
    (-78.4908, 38.0372, "Shenandoah Joe - Preston Ave"),
    (-78.4930, 38.0322, "Milli Coffee - West Main"),
    (-78.5065, 38.0505, "Greenberry's - Barracks Road"),
    (-78.5165, 38.0105, "Cafe - Fifth Street Station"),
    (-78.4370, 38.1290, "Starbucks - Hollymead"),
    (-78.5960, 38.0570, "Cafe - Ivy"),
    (-78.6995, 38.0699, "Grit Coffee - Crozet"),
    (-78.6975, 38.0695, "Mudhouse - Crozet"),
]

# GEOADD: flatten tuples into [lon, lat, member, lon, lat, member, ...]
r.geoadd(KEY, [v for shop in shops for v in shop])
print(f"Added {r.zcard(KEY)} shops to {KEY}")

# GEOSEARCH: shops within 5 km of the UVA Rotunda, closest first, with distances
results = r.geosearch(
    KEY,
    longitude=-78.5034,
    latitude=38.0356,
    radius=5,
    unit="km",
    sort="ASC",
    withdist=True,
)

print("\nCoffee within 5 km of the Rotunda:")
for name, dist in results:
    print(f"  {dist:>6.2f} km  {name}")
