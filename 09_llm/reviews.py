"""
Classify restaurant review sentiment with a local Docker Model Runner 
model and store results in DuckDB.

Docker-hosted models:
https://hub.docker.com/u/ai
docker model pull ai/smollm2   # 256.35 MiB
docker model pull ai/llama3.2  # 1.87 GiB

Or other GGUF models in HF:
docker model pull hf.co/bartowski/Llama-3.2-1B-Instruct-GGUF  # 762.81 MiB
"""

import csv
import json
import os

import duckdb
import httpx

API_URL = "http://127.0.0.1:12434/engines/v1/chat/completions"
DEFAULT_MODEL = "ai/smollm2"
# DEFAULT_MODEL = "ai/llama3.2"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(HERE, "restaurant_reviews.csv")
DEFAULT_DB = os.path.join(HERE, "reviews.duckdb")
LABELS = ("positive", "negative", "neutral")
SCORES = {"positive": 1, "neutral": 0, "negative": -1}

SYSTEM_PROMPT = (
    "You classify the overall sentiment of restaurant reviews. "
    "Respond with exactly one word: positive, negative, or neutral. "
    "Never answer 'mixed'. If a review has both praise and criticism, choose the side "
    "that dominates; if they are evenly balanced, answer neutral."
)


def classify(client: httpx.Client, model: str, text: str, show_payload: bool = False) -> str:
    """Return one of LABELS for the given review text."""
    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
    }
    if show_payload:
        print(f"POST {API_URL}\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n")
    resp = client.post(API_URL, json=payload)
    resp.raise_for_status()
    answer = resp.json()["choices"][0]["message"]["content"].strip().lower()
    # Models sometimes add punctuation or extra words; take the first label mentioned.
    for label in LABELS:
        if label in answer:
            return label
    if "mixed" in answer:
        return "neutral"
    raise ValueError(f"Unexpected model response: {answer!r}")


def main() -> None:
    with open(DEFAULT_CSV, newline="", encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    rows = []
    with httpx.Client(timeout=120) as client:
        for i, rec in enumerate(records):
            sentiment = classify(client, DEFAULT_MODEL, rec["Review comments"], show_payload=(i == 0))
            print(f"{rec['Restaurant']}: {sentiment}")
            rows.append((rec["Date of Visit"], rec["Restaurant"], rec["Location"],
                         rec["Review comments"], sentiment, SCORES[sentiment]))

    con = duckdb.connect(DEFAULT_DB)
    con.execute("""
        CREATE OR REPLACE TABLE reviews (
            date_of_visit DATE,
            restaurant VARCHAR,
            location VARCHAR,
            review_comments VARCHAR,
            sentiment VARCHAR CHECK (sentiment IN ('positive', 'negative', 'neutral')),
            sentiment_score INTEGER CHECK (sentiment_score IN (1, 0, -1))
        )
    """)
    con.executemany("INSERT INTO reviews VALUES (?, ?, ?, ?, ?, ?)", rows)
    con.close()
    print(f"Wrote {len(rows)} rows to {DEFAULT_DB} (table: reviews)")


if __name__ == "__main__":
    main()
