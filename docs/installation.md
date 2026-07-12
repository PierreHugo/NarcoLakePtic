# Installation & Build Procedures

> Complete setup guide for NarcoLakePtic — from prerequisites to running the full pipeline.

---

## 1. Prerequisites

### 1.1 Required Software

| Tool | Minimum Version | Verify Installation |
|------|----------------|---------------------|
| **Docker** | 24.0+ | `docker --version` |
| **Docker Compose** | 2.20+ | `docker compose version` |
| **Git** | 2.30+ | `git --version` |
| **Python** | 3.11+ (for local dev) | `python3 --version` |

### 1.2 System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 4 GB | 8 GB+ |
| CPU | 2 cores | 4+ cores |
| Disk | 10 GB free | 20 GB+ free |
| Network | Internet (for openFDA API) | Stable broadband |

### 1.3 Optional (for local development)

| Tool | Purpose |
|------|---------|
| `python3-venv` | Virtual environments |
| `make` | Build automation |
| `jq` | JSON parsing in shell |
| `httpie` | Better HTTP client than curl |

---

## 2. Quick Start (Docker Compose)

### 2.1 Clone Repository

```bash
git clone https://github.com/PierreHugo/NarcoLakePtic
cd NarcoLakePtic
```

### 2.2 Configure Environment

```bash
# Copy template and edit
cp .env.example .env

# Edit with your settings (optional - defaults work for local)
# Required: OPENFDA_API_KEY for higher rate limits (free at open.fda.gov)
vim .env
```

Example `.env`:
```bash
# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_RAW=raw

# openFDA (optional - get free key at https://open.fda.gov/apis/authentication/)
OPENFDA_API_KEY=your_key_here

# API Gateway
API_HOST=0.0.0.0
API_PORT=8000

# Data paths (inside containers)
DATA_STAGING_PATH=/opt/airflow/data/staging
DATA_CURATED_PATH=/opt/airflow/data/curated
```

### 2.3 Start All Services

```bash
# Build and start in background
docker-compose up -d --build

# Check status
docker-compose ps

# Follow logs
docker-compose logs -f
```

**Expected services**:
- `narcolakeptic-minio` — Object storage (ports 9000, 9001)
- `narcolakeptic-postgres` — Airflow metadata DB (port 5432)
- `narcolakeptic-airflow-webserver` — Airflow UI (port 8080)
- `narcolakeptic-airflow-scheduler` — Airflow scheduler
- `narcolakeptic-api` — FastAPI Gateway (port 8000)

### 2.4 Initialize MinIO Buckets

Buckets are auto-created by `init-minio` service on startup. Verify:

```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# Open MinIO Console
# http://localhost:9001 (minioadmin / minioadmin)

# Verify buckets exist
docker-compose exec minio mc ls myminio/
```

### 2.5 Initialize Airflow

Airflow initializes automatically. Wait for webserver to be healthy (~60s):

```bash
# Check Airflow is ready
docker-compose exec airflow-webserver airflow version

# Open Airflow UI
# http://localhost:8080
# Login: airflow / airflow (or check _AIRFLOW_WEBSERVER_KEY in docker-compose)
```

### 2.6 Trigger Pipeline

**Option A: Via Airflow UI**
1. Open http://localhost:8080
2. Find `narcolakeptic_pipeline` DAG
3. Click **Trigger DAG** (▶️ button)

**Option B: Via CLI**
```bash
docker-compose exec airflow-webserver airflow dags trigger narcolakeptic_pipeline
```

### 2.7 Monitor Execution

- **Airflow UI**: Graph view → click tasks for logs
- **API Health**: `curl http://localhost:8000/health`
- **Pipeline Stats**: `curl http://localhost:8000/stats | jq`

### 2.8 Verify Outputs

```bash
# Check curated files exist
docker-compose exec api-gateway ls -la /app/data/curated/

# Quick data inspection
docker-compose exec api-gateway python -c "
import pandas as pd
for f in ['drug_catalog', 'review_analytics', 'substance_profiles', 'combined_analysis']:
    df = pd.read_parquet(f'/app/data/curated/{f}.parquet')
    print(f'{f}: {len(df)} rows, {len(df.columns)} cols')
"
```

---

## 3. Local Development Setup

For active development without Docker overhead.

### 3.1 Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

### 3.2 Configure Local Environment

```bash
cp .env.example .env.local
# Edit to point to running Docker services:
# MINIO_ENDPOINT=localhost:9000
# DATA_STAGING_PATH=data/staging
# DATA_CURATED_PATH=data/curated
```

### 3.3 Run Services Individually

```bash
# Terminal 1: Start MinIO only
docker-compose up -d minio postgres init-minio

# Terminal 2: Start API Gateway
uvicorn api.gateway:app --host 0.0.0.0 --port 8000 --reload

# Terminal 3: Run pipeline steps manually
python -m ingestion.fetch_druglib
python -m ingestion.fetch_openfda
python -m processing.raw_to_staging
python -m processing.staging_to_curated
```

### 3.4 Run Tests

```bash
# Install test deps
pip install pytest pytest-cov pytest-mock

# Run tests
pytest tests/ -v --cov=.
```

---

## 4. Production Deployment

### 4.1 Security Hardening

```bash
# 1. Change default secrets in .env
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)
_AIRFLOW_WEBSERVER_KEY=$(openssl rand -hex 32)
MINIO_ACCESS_KEY=$(openssl rand -hex 16)
MINIO_SECRET_KEY=$(openssl rand -hex 32)

# 2. Use Docker secrets for production
# echo "your-secret" | docker secret create minio_secret -

# 3. Enable HTTPS for API
# - Add reverse proxy (nginx/traefik) with TLS
# - Set MINIO_SECURE=true
```

### 4.2 Resource Limits

```yaml
# Add to docker-compose.yml services:
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 4G
    reservations:
      cpus: '1'
      memory: 2G
```

### 4.3 Persistence

```yaml
# Ensure named volumes are used (already in compose)
volumes:
  minio_data:      # Raw data
  postgres_data:   # Airflow metadata
```

### 4.4 Backup Strategy

```bash
# Backup script (run via cron)
#!/bin/bash
DATE=$(date +%Y%m%d)
BACKUP_DIR=/backups/$DATE

mkdir -p $BACKUP_DIR

# Curated data
cp -r data/curated $BACKUP_DIR/

# Airflow metadata (optional)
docker-compose exec postgres pg_dump -U airflow airflow > $BACKUP_DIR/airflow.sql

# MinIO bucket
docker-compose exec minio mc mirror myminio/raw $BACKUP_DIR/raw/

# Compress
tar -czf /backups/narcolakeptic-$DATE.tar.gz -C /backups/$DATE .
```

---

## 5. Troubleshooting

### 5.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `Airflow webserver unhealthy` | Postgres not ready | Wait 60s, check `docker-compose logs postgres` |
| `MinIO connection refused` | MinIO not healthy | Check `docker-compose logs minio`, verify healthcheck |
| `openFDA rate limited` | No API key / too many requests | Add `OPENFDA_API_KEY` to `.env`, increase `DELAY` in `fetch_openfda.py` |
| `No space left on device` | Docker volumes full | `docker system prune -a`, increase disk |
| `Permission denied` on data dirs | UID mismatch | `sudo chown -R $USER:$USER data/` |

### 5.2 Debug Commands

```bash
# Check service logs
docker-compose logs -f [service-name]

# Execute shell in container
docker-compose exec api-gateway bash
docker-compose exec airflow-webserver bash

# Check disk usage
docker system df
df -h

# Reset everything (DESTRUCTIVE)
docker-compose down -v
docker system prune -a
```

### 5.3 Airflow Specific

```bash
# Reset Airflow DB (DESTRUCTIVE)
docker-compose exec airflow-webserver airflow db reset -y

# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Test task locally
docker-compose exec airflow-webserver airflow tasks test narcolakeptic_pipeline fetch_druglib 2025-01-01
```

---

## 6. Uninstall / Cleanup

```bash
# Stop and remove containers, networks, volumes
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Remove all unused Docker resources
docker system prune -a --volumes
```

---

## 7. Support

- **Issues**: GitHub Issues
- **Documentation**: `docs/` directory
- **Architecture**: `docs/architecture.md`
- **Usage Guide**: `docs/usage.md`
- **Technical Choices**: `docs/tech-choices.md`

---

*Last updated: 2026-07-12*
*Version: 1.0*