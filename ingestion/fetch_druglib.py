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
import logging

import requests
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

DRUGLIB_URL = "https://archive.ics.uci.edu/static/public/461/data.csv"
RAW_LOCAL_PATH = "data/raw/druglib_reviews_static.csv"
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

EXPECTED_COLUMNS = [
    "reviewID", "urlDrugName", "rating", "effectiveness",
    "sideEffects", "condition", "benefitsReview",
    "sideEffectsReview", "commentsReview",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Create and return MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )


def ensure_bucket(client: Minio, bucket: str):
    """Ensure bucket exists, create if not."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Created bucket: {bucket}")
    else:
        log.info(f"Bucket exists: {bucket}")


def fetch_druglib():
    """Download Druglib dataset and store in MinIO raw bucket."""
    log.info(f"Fetching Druglib from {DRUGLIB_URL}")

    response = requests.get(DRUGLIB_URL, timeout=60)
    response.raise_for_status()

    # Validate header
    reader = csv.reader(io.StringIO(response.text))
    header = next(reader)
    if header != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected columns: {header}")

    row_count = sum(1 for _ in reader)

    # Save local copy for staging pipeline
    os.makedirs(os.path.dirname(RAW_LOCAL_PATH), exist_ok=True)
    with open(RAW_LOCAL_PATH, "w", encoding="utf-8", newline="") as f:
        f.write(response.text)

    log.info(f"Druglib downloaded: {row_count} rows -> {RAW_LOCAL_PATH}")

    # Push to MinIO raw bucket
    try:
        client = get_minio_client()
        ensure_bucket(client, MINIO_BUCKET_RAW)

        object_name = "druglib/druglib_reviews_static.csv"
        data = io.BytesIO(response.content)
        data_size = len(response.content)

        client.put_object(
            MINIO_BUCKET_RAW,
            object_name,
            data,
            data_size,
            content_type="text/csv"
        )
        log.info(f"Pushed to MinIO: s3://{MINIO_BUCKET_RAW}/{object_name} ({data_size} bytes)")

    except Exception as e:
        log.error(f"Failed to push to MinIO: {e}")
        raise

    return row_count


if __name__ == "__main__":
    fetch_druglib()