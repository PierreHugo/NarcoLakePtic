# 🌿 NarcoLakePtic

> A Data Lake project analyzing global drug trends using open public data sources.

![Python](https://img.shields.io/badge/Python-3.14+-green)
![Prefect](https://img.shields.io/badge/Orchestration-Prefect-blue)
![MinIO](https://img.shields.io/badge/Storage-MinIO-purple)
![Docker](https://img.shields.io/badge/Infra-Docker-orange)

NarcoLakePtic is a Data Lake pipeline that ingests, transforms and exposes drug-related data from two official public sources:
- **Druglib.com** (via UCI ML Repository) — Patient drug reviews with ratings, effectiveness, side effects, and conditions
- **openFDA /drug/label API** — Official FDA drug labeling data (indications, warnings, adverse reactions, interactions)

Built by 3 students at EFREI Paris — Big Data & Machine Learning track (2025-2026).

---

## Architecture

```
Raw (MinIO/S3) ──► Staging (Parquet, cleaned) ──► Curated (Parquet, analytics-ready)
                    ▲                            ▲
                    │                            │
              Prefect Flow                 Prefect Flow
              (orchestration)                (orchestration)
```

**Three-layer Data Lake:**
- **Raw** — Immutable source data in MinIO (S3-compatible) bucket `raw`
- **Staging** — Cleaned, normalized Parquet files in `data/staging/`
- **Curated** — Enriched, joined, analytics-ready Parquet datasets in `data/curated/`

---

## Data Sources

| Source | Type | Content | Layer |
|--------|------|---------|-------|
| **Druglib.com** (UCI ML Repo) | Static CSV file | ~215k patient reviews: drug name, condition, rating, effectiveness, side effects, free-text reviews | Raw → Staging → Curated |
| **openFDA /drug/label API** | REST API (paginated) | ~200k drug labels: brand/generic names, manufacturer, substance, indications, warnings, adverse reactions, interactions | Raw → Staging → Curated |

---

## Curated Datasets Produced

| Dataset | Description | Grain |
|---------|-------------|-------|
| `drug_catalog.parquet` | Unique drugs from openFDA with metadata completeness flags | 1 row / drug (brand + substance + manufacturer) |
| `review_analytics.parquet` | Aggregated patient review metrics from Druglib | 1 row / drug name |
| `substance_profiles.parquet` | Enriched substance profiles joining openFDA + Druglib | 1 row / substance |
| `combined_analysis.parquet` | Joined records where generic name matches | 1 row / matched record |

---

## Project Structure

```
NarcoLakePtic/
├── data/
│   ├── raw/          # (MinIO bucket 'raw') — local copy for staging input
│   ├── staging/      # Cleaned Parquet files
│   └── curated/      # Analytics-ready Parquet datasets
├── ingestion/        # Source ingestion scripts
│   ├── fetch_druglib.py     # Downloads UCI CSV to MinIO raw
│   └── fetch_openfda.py     # Paginated openFDA API fetch with date partitioning
├── processing/       # Layer transformations
│   ├── raw_to_staging.py    # Raw → Staging: cleaning, normalization, Parquet
│   └── staging_to_curated.py # Staging → Curated: aggregations, joins, enrichment
├── flows/            # Prefect orchestration
│   └── main_flow.py         # End-to-end pipeline flow
├── notebooks/        # Exploration & visualization
├── docs/             # Documentation
├── docker-compose.yml      # MinIO + Prefect services
├── requirements.txt        # Python dependencies
├── .env.example            # Environment template
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- `python3-venv` recommended

### 1. Clone & Configure
```bash
git clone https://github.com/PierreHugo/NarcoLakePtic
cd NarcoLakePtic
cp .env.example .env
```

### 2. Start Infrastructure
```bash
docker-compose up -d
# MinIO console: http://localhost:9001 (minioadmin / minioadmin)
# Prefect UI:    http://localhost:4200
```

### 3. Initialize MinIO Buckets
```bash
# Create raw bucket (run once)
python -c "
from minio import Minio
client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
if not client.bucket_exists('raw'):
    client.make_bucket('raw')
    print('Bucket raw created')
else:
    print('Bucket raw exists')
"
```

### 4. Create Virtual Environment & Install
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Run the Pipeline

**Option A: Full Prefect Flow (recommended)**
```bash
prefect server start  # In another terminal
python -m flows.main_flow
```

**Option B: Step-by-step**
```bash
# 1. Ingest raw data to MinIO
python -m ingestion.fetch_druglib
python -m ingestion.fetch_openfda

# 2. Transform Raw → Staging
python -m processing.raw_to_staging

# 3. Transform Staging → Curated
python -m processing.staging_to_curated
```

---

## Output Verification

```bash
# Check curated outputs
ls -la data/curated/
# drug_catalog.parquet
# review_analytics.parquet
# substance_profiles.parquet
# combined_analysis.parquet

# Quick inspection
python -c "
import pandas as pd
for f in ['drug_catalog', 'review_analytics', 'substance_profiles', 'combined_analysis']:
    df = pd.read_parquet(f'data/curated/{f}.parquet')
    print(f'{f}: {len(df)} rows, {len(df.columns)} cols')
    print(df.head(2))
    print()
"
```

---

## Configuration (`.env`)

```bash
# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_RAW=raw

# openFDA (optional - public API, no key required for basic use)
OPENFDA_API_KEY=

# Prefect
PREFECT_API_URL=http://localhost:4200/api
```

---

## API Gateway (Planned - PDF §2.5)

> **Required by project specification**

| Endpoint | Description | Status |
|----------|-------------|--------|
| `GET /raw` | Access raw data listings/files | 🔲 Planned |
| `GET /staging` | Access staging Parquet datasets | 🔲 Planned |
| `GET /curated` | Access curated analytics datasets | 🔲 Planned |
| `GET /health` | Service health check | 🔲 Planned |
| `GET /stats` | Bucket sizes, row counts, freshness | 🔲 Planned |

---

## Team

EFREI Paris — ING2-APP-BDML1, 2025-2026
- [Pierre-Hugo HERRAN](https://github.com/PierreHugo)
- [Uthum UKWATTAGE](https://github.com/uthumatik)
- [Aymen Alloune](https://github.com/AymenShe)

---

## Data Usage & License

- **Druglib.com**: UCI ML Repository — Non-commercial / research use only
- **openFDA**: Public domain (U.S. Government works) — No restrictions

> Public data only. For academic / portfolio purposes.

---

## Project Status

| Layer | Status | Notes |
|-------|--------|-------|
| Raw (MinIO ingestion) | ✅ Implemented | Druglib + openFDA fetch scripts |
| Staging (cleaning) | ✅ Implemented | Parquet, normalized schemas |
| Curated (analytics) | ✅ Implemented | 4 enriched datasets with joins |
| Orchestration (Prefect) | ✅ Implemented | End-to-end flow |
| API Gateway | 🔲 **TODO** | Required by spec (§2.5) |
| CI/CD (DVC/Airflow) | 🔲 **TODO** | Required by spec (§2.4) |
| Tests & Validation | 🔲 **TODO** | Required by eval (§4.1) |
| Documentation | 🟡 Partial | This README updated |

---

## License

MIT License — See [LICENSE](LICENSE)