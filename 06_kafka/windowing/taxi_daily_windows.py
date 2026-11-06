import duckdb
import os
import json

def import_daily(): 
    try:
        conn = duckdb.connect('daily.db')
        conn.execute("CREATE TABLE IF NOT EXISTS daily AS SELECT * FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-08.parquet')")
    except Exception as e:
        print(e)
    finally:
        conn.close()

def generate_daily_summary():
    try:
        conn = duckdb.connect('daily.db')
        # Extract day dynamically in the query
        daily_summary = conn.execute("SELECT EXTRACT(DAY FROM tpep_pickup_datetime) AS day, count(*) AS count FROM daily GROUP BY day ORDER BY day")
        # save the result to a JSON dict and pretty print it
        print(json.dumps(daily_summary.fetchall(), indent=4))
        # save the result to a new duckdb table named daily_summary
        conn.execute("CREATE TABLE daily_summary AS SELECT EXTRACT(DAY FROM tpep_pickup_datetime) AS day, count(*) AS count FROM daily GROUP BY day ORDER BY day")
    except Exception as e:
        print(e)
    finally:
        conn.close()

if __name__ == "__main__":
    import_daily()
    generate_daily_summary()