import duckdb
import os
import json

def generate_hourly_summary():
    try:
        conn = duckdb.connect('daily.db')
        # Extract day dynamically in the query
        hourly_summary = conn.execute("""
            SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, count(*) AS count 
            FROM daily 
            WHERE DATE(tpep_pickup_datetime) = '2025-08-10' 
            GROUP BY hour 
            ORDER BY hour
        """)
        # save the result to a JSON dict and pretty print it
        print(json.dumps(hourly_summary.fetchall(), indent=4))
        # save the result to a new duckdb table named daily_summary
        conn.execute("""
        CREATE OR REPLACE TABLE hourly_summary AS 
            SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, count(*) AS count 
            FROM daily 
            WHERE DATE(tpep_pickup_datetime) = '2025-08-10' 
            GROUP BY hour 
            ORDER BY hour
        """)
    except Exception as e:
        print(e)
    finally:
        conn.close()

if __name__ == "__main__":
    generate_hourly_summary()