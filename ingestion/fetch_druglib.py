"""
Ingestion Druglib.com - télécharge le dataset statique UCI ML Repository
(avis patients sur médicaments : efficacité, effets secondaires, condition)
et le pousse dans MinIO (raw)

Source: https://archive.ics.uci.edu/dataset/461/drug+review+dataset+druglib+com
Usage recherche/non-commercial uniquement (licence du dataset).
"""
import csv
import io
import os

import requests
from dotenv import load_dotenv

load_dotenv()

DRUGLIB_URL = "https://archive.ics.uci.edu/static/public/461/data.csv"
RAW_OUTPUT_PATH = "data/raw/druglib_reviews_static.csv"

EXPECTED_COLUMNS = [
    "reviewID", "urlDrugName", "rating", "effectiveness",
    "sideEffects", "condition", "benefitsReview",
    "sideEffectsReview", "commentsReview",
]


def fetch_druglib():
    response = requests.get(DRUGLIB_URL, timeout=30)
    response.raise_for_status()

    reader = csv.reader(io.StringIO(response.text))
    header = next(reader)
    row_count = sum(1 for _ in reader)

    if header != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected columns: {header}")

    os.makedirs(os.path.dirname(RAW_OUTPUT_PATH), exist_ok=True)
    with open(RAW_OUTPUT_PATH, "w", encoding="utf-8", newline="") as f:
        f.write(response.text)

    # TODO: pousser RAW_OUTPUT_PATH dans le bucket MinIO raw
    print(f"Druglib fetched: {row_count} rows -> {RAW_OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_druglib()
