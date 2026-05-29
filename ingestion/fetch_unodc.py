"""
Ingestion UNODC - télécharge le dataset CSV et le pousse dans MinIO (raw)
TODO: remplacer l'URL par le vrai endpoint UNODC
"""
import requests
from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

UNODC_URL = "https://placeholder.unodc.org/data.csv"

def fetch_unodc():
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    response = requests.get(UNODC_URL)
    # TODO: sauvegarder dans MinIO bucket raw
    print("UNODC fetched, status:", response.status_code)

if __name__ == "__main__":
    fetch_unodc()
