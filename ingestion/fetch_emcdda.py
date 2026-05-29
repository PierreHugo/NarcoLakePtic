"""
Ingestion EMCDDA - appelle l'API et pousse dans MinIO (raw)
TODO: ajouter la vraie clé API EMCDDA
"""
import requests
from dotenv import load_dotenv
import os

load_dotenv()

EMCDDA_BASE_URL = "https://www.emcdda.europa.eu/api/placeholder"

def fetch_emcdda():
    # TODO: paramétrer les endpoints selon les données voulues
    response = requests.get(EMCDDA_BASE_URL)
    print("EMCDDA fetched, status:", response.status_code)

if __name__ == "__main__":
    fetch_emcdda()
