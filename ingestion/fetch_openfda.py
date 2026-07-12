"""
Ingestion openFDA - appelle l'API /drug/label et pousse dans MinIO (raw)

Récupère les fiches médicament complètes (pas seulement les noms) :
marque, générique, substance, fabricant, indications, mises en garde,
posologie, ingrédients actifs, contre-indications, interactions,
effets indésirables.

L'endpoint /drug/label plafonne `skip` à 25000 (pagination classique
insuffisante pour couvrir les ~260k fiches). On contourne la limite en
partitionnant les requêtes par plage de `effective_time`, en découpant
récursivement chaque plage tant qu'elle dépasse 25000 résultats.

Optionnel - clé API gratuite sur https://open.fda.gov/apis/authentication/
pour passer de 40 à 240 req/min :
  export OPENFDA_API_KEY="your_key_here"
"""
import csv
import logging
import os
import time
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL      = "https://api.fda.gov/drug/label.json"
PARTITION_CAP = 25000         # openFDA's max skip value
PAGE_SIZE     = 1000          # max limit per call
DELAY         = 0.4           # seconds between requests (respect rate limit)
MAX_RETRIES   = 3
API_KEY       = os.getenv("OPENFDA_API_KEY", "")

RAW_OUTPUT_PATH = "data/raw/openfda_drug_labels_api.csv"

EARLIEST = date(1900, 1, 1)
LATEST   = date(2030, 12, 31)

FIELDNAMES = [
    "spl_id", "brand_name", "generic_name", "manufacturer_name",
    "substance_name", "product_type", "route", "effective_time",
    "indications_and_usage", "warnings", "dosage_and_administration",
    "active_ingredient", "do_not_use", "contraindications",
    "drug_interactions", "adverse_reactions",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get(params: dict) -> dict | None:
    """GET the openFDA API with retries. Returns parsed JSON or None."""
    if API_KEY:
        params["api_key"] = API_KEY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=20)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                wait = 60
                log.warning("Rate limited — waiting %ds", wait)
                time.sleep(wait)
                continue

            log.warning("HTTP %d on attempt %d", r.status_code, attempt)

        except requests.RequestException as e:
            log.warning("Request error attempt %d: %s", attempt, e)

        if attempt < MAX_RETRIES:
            time.sleep(DELAY * (2 ** attempt))

    return None


def range_query(lo: date, hi: date) -> str:
    return f"effective_time:[{lo:%Y%m%d} TO {hi:%Y%m%d}]"


def total_for_range(lo: date, hi: date) -> int:
    data = get({"search": range_query(lo, hi), "limit": 1})
    if not data:
        return 0
    return data.get("meta", {}).get("results", {}).get("total", 0)


def iter_ranges(lo: date, hi: date):
    """Recursively split [lo, hi] until each leaf has <= PARTITION_CAP records."""
    total = total_for_range(lo, hi)
    if total == 0:
        return
    if total <= PARTITION_CAP or hi <= lo:
        yield (lo, hi, total)
        return

    mid = lo + (hi - lo) // 2
    if mid <= lo:
        # can't split any further (single-day range) — accept the cap
        yield (lo, hi, total)
        return

    yield from iter_ranges(lo, mid)
    yield from iter_ranges(mid + timedelta(days=1), hi)


def field(rec: dict, *keys: str) -> str:
    """Return the first non-empty value among `keys`, joining list values."""
    for key in keys:
        v = rec.get(key)
        if v:
            return " | ".join(v) if isinstance(v, list) else str(v)
    return ""


def openfda_field(rec: dict, key: str) -> str:
    v = rec.get("openfda", {}).get(key)
    return " | ".join(v) if isinstance(v, list) else ""


def extract_row(rec: dict) -> dict:
    return {
        "spl_id":                     openfda_field(rec, "spl_id"),
        "brand_name":                 openfda_field(rec, "brand_name"),
        "generic_name":               openfda_field(rec, "generic_name"),
        "manufacturer_name":          openfda_field(rec, "manufacturer_name"),
        "substance_name":             openfda_field(rec, "substance_name"),
        "product_type":               openfda_field(rec, "product_type"),
        "route":                      openfda_field(rec, "route"),
        "effective_time":             rec.get("effective_time", ""),
        "indications_and_usage":      field(rec, "indications_and_usage", "purpose"),
        "warnings":                   field(rec, "warnings", "warnings_and_cautions", "boxed_warning"),
        "dosage_and_administration":  field(rec, "dosage_and_administration"),
        "active_ingredient":          field(rec, "active_ingredient"),
        "do_not_use":                 field(rec, "do_not_use"),
        "contraindications":          field(rec, "contraindications"),
        "drug_interactions":          field(rec, "drug_interactions"),
        "adverse_reactions":          field(rec, "adverse_reactions"),
    }


def fetch_openfda():
    os.makedirs(os.path.dirname(RAW_OUTPUT_PATH), exist_ok=True)
    seen_ids: set[str] = set()
    row_count = 0

    with open(RAW_OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()

        for lo, hi, total in iter_ranges(EARLIEST, LATEST):
            log.info("Partition %s..%s — %d records", lo, hi, total)
            skip = 0
            while skip < min(total, PARTITION_CAP):
                time.sleep(DELAY)
                data = get({"search": range_query(lo, hi), "limit": PAGE_SIZE, "skip": skip})
                if not data:
                    break
                for rec in data.get("results", []):
                    rid = rec.get("id")
                    if not rid or rid in seen_ids:
                        continue
                    seen_ids.add(rid)
                    writer.writerow(extract_row(rec))
                    row_count += 1
                skip += PAGE_SIZE

    print(f"openFDA fetched: {row_count} rows -> {RAW_OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_openfda()
