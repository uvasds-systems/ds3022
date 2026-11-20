"""Classify restaurant review sentiment with a local Docker Model Runner model (via the openai package) and store results in DuckDB."""

import csv
import json
import os

import duckdb
from openai import OpenAI

# The openai client appends "/chat/completions" itself, so give it only the base URL.
BASE_URL = "http://127.0.0.1:12434/engines/v1"
DEFAULT_MODEL = "ai/llama3.2"
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


def classify(client: OpenAI, model: str, text: str, show_payload: bool = False) -> str:
    """Return one of LABELS for the given review text."""
    request = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ],
    }
    if show_payload:
        print(f"POST {BASE_URL}/chat/completions\n{json.dumps(request, indent=2, ensure_ascii=False)}\n")
    # The SDK builds the same POST body as reviews.py and returns a typed response object.
    resp = client.chat.completions.create(**request)
    answer = resp.choices[0].message.content.strip().lower()
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

    # Model Runner ignores the API key, but the SDK requires a non-empty value.
    client = OpenAI(base_url=BASE_URL, api_key="not-needed", timeout=120)

    rows = []
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
