import duckdb, json

con = duckdb.connect("my.db")
con.execute("CREATE TABLE IF NOT EXISTS raw_events (payload JSON)")

blobs = [{"user": "alice", "score": 1}, {"user": "bob", "score": 2}]
con.executemany(
    "INSERT INTO raw_events VALUES (?)",
    [(json.dumps(b),) for b in blobs],
)
