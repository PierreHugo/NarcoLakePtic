# NarcoLakePtic Usage Guide

> Complete guide for using the NarcoLakePtic data lake pipeline and API.

---

## Table of Contents
1. [Pipeline Execution](#1-pipeline-execution)
2. [API Reference](#2-api-reference)
3. [Data Exploration](#3-data-exploration)
4. [Monitoring & Scheduling](#4-monitoring--scheduling)
5. [Common Workflows](#5-common-workflows)

---

## 1. Pipeline Execution

### 1.1 Via Airflow (Production)

```bash
# Start services
docker-compose up -d

# Trigger full pipeline
docker-compose exec airflow-webserver airflow dags trigger narcolakeptic_pipeline

# Monitor in UI: http://localhost:8080
```

**DAG Structure**:
```
health_check
    ├── fetch_druglib
    ├── fetch_openfda
    ├── raw_to_staging_druglib
    ├── raw_to_staging_openfda
    ├── staging_to_curated
    └── pipeline_summary
```

**Schedule**: `@daily` (configurable in DAG)

### 1.2 Manual Step-by-Step (Development)

```bash
# 1. Ingest Raw Data → MinIO
python -m ingestion.fetch_druglib
python -m ingestion.fetch_openfda

# 2. Raw → Staging (cleaning, Parquet)
python -m processing.raw_to_staging

# 3. Staging → Curated (aggregations, joins)
python -m processing.staging_to_curated

# 4. Verify outputs
ls -la data/curated/
# drug_catalog.parquet
# review_analytics.parquet
# substance_profiles.parquet
# combined_analysis.parquet
```

### 1.3 With Custom Parameters

```bash
# Skip validation (faster)
python -m processing.raw_to_staging --no-validate
python -m processing.staging_to_curated --no-validate

# Process only one source
python -m processing.raw_to_staging --druglib-only
python -m processing.raw_to_staging --openfda-only
```

---

## 2. API Reference

**Base URL**: `http://localhost:8000`

**Interactive Docs**: `http://localhost:8000/docs` (Swagger UI)

### 2.1 System Endpoints

#### `GET /health`
Health check for all dependencies.

```bash
curl http://localhost:8000/health
```

**Response**:
```json
{
  "status": "healthy",
  "service": "NarcoLakePtic API Gateway",
  "version": "1.0.0",
  "minio_connected": true,
  "staging_accessible": true,
  "curated_accessible": true
}
```

#### `GET /stats`
Storage and row count statistics.

```bash
curl http://localhost:8000/stats
```

**Response**:
```json
{
  "raw_bucket": {
    "bucket": "raw",
    "object_count": 2,
    "total_size_bytes": 123456789,
    "total_size_mb": 117.73,
    "objects": [...]
  },
  "staging": {
    "path": "data/staging",
    "file_count": 2,
    "total_size_bytes": 45678900,
    "total_size_mb": 43.56,
    "total_rows": 350000,
    "files": [...]
  },
  "curated": {
    "path": "data/curated",
    "file_count": 4,
    "total_size_bytes": 23456789,
    "total_size_mb": 22.37,
    "total_rows": 125000,
    "files": [...]
  }
}
```

---

### 2.2 Raw Layer Endpoints

#### `GET /raw`
List objects in MinIO raw bucket.

```bash
# List all
curl http://localhost:8000/raw

# Filter by prefix
curl "http://localhost:8000/raw?prefix=druglib/"

# Limit results
curl "http://localhost:8000/raw?limit=50"
```

**Response**:
```json
{
  "bucket": "raw",
  "prefix": "",
  "count": 2,
  "objects": [
    {
      "name": "druglib/druglib_reviews_static.csv",
      "size_bytes": 45678900,
      "last_modified": "2025-01-15T10:30:00",
      "etag": "abc123..."
    }
  ]
}
```

#### `GET /raw/{object_name}`
Download a specific raw file.

```bash
curl http://localhost:8000/raw/druglib/druglib_reviews_static.csv -o druglib_raw.csv
```

#### `GET /raw?download=1&path={object_name}`
Alternative download syntax.

---

### 2.3 Staging Layer Endpoints

#### `GET /staging`
List staging Parquet files with metadata.

```bash
curl http://localhost:8000/staging
```

**Response**:
```json
{
  "path": "data/staging",
  "file_count": 2,
  "files": [
    {
      "name": "druglib.parquet",
      "size_bytes": 23456789,
      "size_mb": 22.37,
      "rows": 150000,
      "columns": 9,
      "column_names": ["reviewID", "drug_name", "rating", ...],
      "dtypes": {"reviewID": "object", "rating": "float64", ...}
    }
  ]
}
```

#### `GET /staging?file={filename}&limit=100`
Get sample from specific file.

```bash
curl "http://localhost:8000/staging?file=druglib.parquet&limit=10"
```

#### `GET /staging/{filename}`
Get data from staging file with pagination.

```bash
# JSON (default), first 100 rows
curl "http://localhost:8000/staging/druglib.parquet?limit=100&offset=0"

# CSV export
curl "http://localhost:8000/staging/druglib.parquet?format=csv&limit=1000" -o druglib_staging.csv

# Parquet export
curl "http://localhost:8000/staging/druglib.parquet?format=parquet" -o druglib_staging.parquet
```

**Query Parameters**:
| Parameter | Default | Description |
|-----------|---------|-------------|
| `limit` | 1000 | Max rows to return |
| `offset` | 0 | Row offset for pagination |
| `format` | `json` | Output: `json`, `csv`, `parquet` |

**JSON Response**:
```json
{
  "file": "druglib.parquet",
  "total_rows": 150000,
  "offset": 0,
  "limit": 100,
  "returned": 100,
  "columns": ["reviewID", "drug_name", "rating", ...],
  "data": [
    {"reviewID": "1", "drug_name": "Aspirin", "rating": 4.5, ...},
    ...
  ]
}
```

---

### 2.4 Curated Layer Endpoints

#### `GET /curated`
List curated Parquet files.

```bash
curl http://localhost:8000/curated
```

#### `GET /curated?file={filename}&limit=100`
Get sample from curated file.

```bash
curl "http://localhost:8000/curated?file=drug_catalog.parquet&limit=5"
```

#### `GET /curated/{filename}`
Get curated data with pagination and format options.

```bash
# Drug catalog - first 50 rows as JSON
curl "http://localhost:8000/curated/drug_catalog.parquet?limit=50"

# Review analytics - CSV export
curl "http://localhost:8000/curated/review_analytics.parquet?format=csv&limit=1000" -o reviews.csv

# Substance profiles - Parquet download
curl "http://localhost:8000/curated/substance_profiles.parquet?format=parquet" -o profiles.parquet

# Combined analysis - paginated
curl "http://localhost:8000/curated/combined_analysis.parquet?limit=100&offset=100"
```

---

## 3. Data Exploration

### 3.1 With Python/Pandas

```python
import pandas as pd

# Load curated datasets
catalog = pd.read_parquet("data/curated/drug_catalog.parquet")
reviews = pd.read_parquet("data/curated/review_analytics.parquet")
profiles = pd.read_parquet("data/curated/substance_profiles.parquet")
combined = pd.read_parquet("data/curated/combined_analysis.parquet")

print(f"Drugs in catalog: {len(catalog)}")
print(f"Drugs with reviews: {len(reviews)}")
print(f"Substances profiled: {len(profiles)}")
print(f"Combined matches: {len(combined)}")

# Top rated drugs
top_drugs = reviews.nlargest(10, 'avg_rating')[['drug_name', 'avg_rating', 'total_reviews']]
print(top_drugs)

# Substances with most products
top_substances = profiles.nlargest(10, 'num_products')[['generic_name', 'num_products', 'avg_rating']]
print(top_substances)
```

### 3.2 With SQL (DuckDB)

```bash
# Install duckdb
pip install duckdb
```

```python
import duckdb

# Query Parquet directly with SQL
conn = duckdb.connect()

# Join catalog with reviews
result = conn.execute("""
    SELECT c.generic_name, c.brand_name, r.avg_rating, r.total_reviews
    FROM 'data/curated/drug_catalog.parquet' c
    JOIN 'data/curated/review_analytics.parquet' r
    ON c.generic_name = r.drug_name
    ORDER BY r.total_reviews DESC
    LIMIT 20
""").fetchdf()

print(result)
```

### 3.3 Via API (from any client)

```bash
# Get top 10 rated drugs with >10 reviews
curl "http://localhost:8000/curated/review_analytics.parquet?limit=1000" | \
  jq '.data | map(select(.total_reviews > 10)) | sort_by(-.avg_rating) | .[:10]'

# Export to CSV for Excel
curl "http://localhost:8000/curated/drug_catalog.parquet?format=csv" -o catalog.csv
```

---

## 4. Monitoring & Scheduling

### 4.1 Airflow Monitoring

**Web UI**: http://localhost:8080
- **DAGs view**: Overall status, last run, next run
- **Graph view**: Task dependencies, execution time
- **Task logs**: Click any task → Log

**CLI Monitoring**:
```bash
# List recent DAG runs
docker-compose exec airflow-webserver airflow dags list-runs -d narcolakeptic_pipeline

# Check task status
docker-compose exec airflow-webserver airflow tasks states-for-dag-run narcolakeptic_pipeline 2025-01-15T00:00:00+00:00

# View logs
docker-compose exec airflow-webserver airflow tasks logs narcolakeptic_pipeline fetch_druglib 2025-01-15T00:00:00+00:00
```

### 4.2 API Health Monitoring

```bash
# Simple health check (for load balancer)
curl -f http://localhost:8000/health || echo "API DOWN"

# Detailed stats
curl http://localhost:8000/stats | jq '.curated.total_rows'
```

### 4.3 Logs

**Airflow logs**: `./logs/` (mounted to `/opt/airflow/logs`)

**API logs**: `docker-compose logs api-gateway`

**MinIO logs**: `docker-compose logs minio`

### 4.4 Custom Scheduling

Edit `dags/narcolakeptic_pipeline.py`:
```python
# Change schedule
schedule_interval="@hourly"  # or "0 2 * * *" for 2 AM daily

# Or disable schedule, trigger manually
schedule_interval=None
```

---

## 5. Common Workflows

### 5.1 Refresh Data Weekly

```bash
# Add to crontab (runs every Monday 3 AM)
0 3 * * 1 cd /path/to/NarcoLakePtic && docker-compose exec airflow-webserver airflow dags trigger narcolakeptic_pipeline
```

### 5.2 Export Data for Analysis

```bash
# Export all curated datasets
mkdir -p exports
for f in drug_catalog review_analytics substance_profiles combined_analysis; do
  curl "http://localhost:8000/curated/${f}.parquet?format=csv" -o "exports/${f}.csv"
done
```

### 5.3 Add New Data Source

1. Create `ingestion/fetch_newsource.py`
2. Add to `raw_to_staging.py`: `process_newsource()`
3. Add to `staging_to_curated.py`: `build_newsource_analytics()`
4. Add tasks to DAG: `fetch_newsource >> raw_to_staging_newsource >> staging_to_curated`
5. Update validation schemas in `dags/utils/validation.py`

### 5.4 Backup Curated Data

```bash
# Backup to timestamped directory
DATE=$(date +%Y%m%d)
mkdir -p backups/$DATE
cp data/curated/*.parquet backups/$DATE/
```

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Start all | `docker-compose up -d` |
| Stop all | `docker-compose down` |
| View logs | `docker-compose logs -f` |
| Trigger pipeline | `docker-compose exec airflow-webserver airflow dags trigger narcolakeptic_pipeline` |
| API docs | `http://localhost:8000/docs` |
| Airflow UI | `http://localhost:8080` |
| MinIO Console | `http://localhost:9001` |
| Health check | `curl http://localhost:8000/health` |
| Get stats | `curl http://localhost:8000/stats \| jq` |
| Export CSV | `curl "http://localhost:8000/curated/drug_catalog.parquet?format=csv" -o out.csv` |

---

*See `docs/installation.md` for setup, `docs/architecture.md` for system design.*