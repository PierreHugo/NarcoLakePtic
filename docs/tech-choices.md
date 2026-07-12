# Technical Choices Justification

> Rationale for technology selections in NarcoLakePtic, aligned with PDF requirements.

---

## Decision Matrix

| Choice | Alternatives Considered | Rationale |
|--------|------------------------|-----------|
| **MinIO for Raw Layer** | Elasticsearch, Local FS, HDFS, AWS S3 | PDF §2.1 requires S3-compatible storage. MinIO provides S3 API on-prem, zero cost, Docker-native. Elasticsearch is for search, not raw storage. Local FS lacks object semantics. HDFS overkill. AWS S3 incurs costs. |
| **Parquet for Staging/Curated** | CSV, Avro, ORC, JSON | Columnar → analytical query performance. Snappy compression (70-90%). Schema evolution. Native Pandas support. CSV lacks types/compression. Avro/ORC add complexity without benefit at this scale. |
| **Airflow for Orchestration** | Prefect (current), DVC, Dagster | PDF §2.4 requires **DVC or Airflow**. Airflow chosen: production-grade, mature UI, XCom, retries, scheduling, SLAs. Prefect simpler but AWS-focused. DVC is version control, not orchestration. Dagster newer, smaller community. |
| **FastAPI for API Gateway** | Flask, Django REST, Starlette, Falcon | PDF §2.5 requires 5 endpoints. FastAPI: async I/O (MinIO, file reads), auto OpenAPI docs, Pydantic validation, type hints, high performance (Uvicorn). Flask sync-only. Django heavyweight. Starlette lower-level. |
| **Pandas for Processing** | Polars, DuckDB, Spark, Dask | Team familiarity, dataset fits in memory (~200k rows × 2 sources). Polars faster but API less familiar. DuckDB great for SQL but adds dependency. Spark/Dask overkill for <1M rows. |
| **MinIO Python SDK** | boto3, s3fs, awscli | Official MinIO SDK, type hints, simple API. boto3 works but AWS-focused. s3fs good for fsspec but extra layer. |
| **PostgreSQL for Airflow** | SQLite, MySQL | Airflow requires Postgres/MySQL for production. SQLite doesn't support parallel scheduler. MySQL equivalent but Postgres more standard. |
| **LocalExecutor for Airflow** | CeleryExecutor, KubernetesExecutor | LocalExecutor sufficient for single-node, no message broker needed. Celery adds RabbitMQ/Redis complexity. K8sExecutor requires Kubernetes cluster. |
| **Python 3.11** | 3.10, 3.12 | 3.11: performance improvements (10-15%), long-term support until 2027. 3.10 EOL sooner. 3.12 too new for some libs. |
| **Docker Compose** | Kubernetes, Nomad, systemd | PDF requires Docker. Compose: simple multi-service, dev/prod parity, single file config. K8s overkill for 3-person project. |

---

## Detailed Justifications

### 1. MinIO over Elasticsearch/Local FS

**Requirement**: PDF §2.1 — "Zone Raw : Stockage objet (MinIO / S3 compatible)"

| Criterion | MinIO | Elasticsearch | Local FS |
|-----------|-------|---------------|----------|
| S3 API Compatible | ✅ Native | ❌ No | ❌ No |
| Object Semantics | ✅ Buckets/keys | ❌ Indices/docs | ❌ Files/dirs |
| HTTP API | ✅ Native | ✅ REST | ❌ No |
| Cost | Free (self-hosted) | Free but heavier | Free |
| Docker Native | ✅ Official image | ✅ | N/A |
| Schema/Metadata | ❌ (add separately) | ✅ Built-in | ❌ |

**Decision**: MinIO is the only option that natively satisfies the S3-compatible requirement.

---

### 2. Parquet over CSV/Avro/ORC

**Requirement**: PDF §2.2 — "Format Parquet pour les zones Staging et Curated"

| Criterion | Parquet | CSV | Avro | ORC |
|-----------|---------|-----|------|-----|
| Columnar | ✅ | ❌ | ❌ | ✅ |
| Compression | ✅ Snappy | ❌ | ✅ | ✅ |
| Schema Evolution | ✅ | ❌ | ✅ | ✅ |
| Pandas Native | ✅ `read_parquet` | ✅ | ❌ | ❌* |
| Type Preservation | ✅ | ❌ | ✅ | ✅ |
| Human Readable | ❌ | ✅ | ❌ | ❌ |

*Pandas ORC support requires `pyarrow` with ORC backend (experimental).

**Decision**: Parquet is the standard for analytical workloads in Python data ecosystem.

---

### 3. Airflow over Prefect/DVC/Dagster

**Requirement**: PDF §2.4 — "Orchestration : DVC **ou** Airflow"

| Criterion | Airflow | Prefect | DVC | Dagster |
|-----------|---------|---------|-----|---------|
| PDF Compliant | ✅ | ❌ | ✅ | ❌ |
| Scheduling | ✅ Cron | ✅ | ❌ (CLI) | ✅ |
| UI/Monitoring | ✅ Rich | ✅ | ❌ | ✅ |
| XCom/Task Comm | ✅ Native | ✅ | ❌ | ✅ |
| Retries/Backoff | ✅ Native | ✅ | Manual | ✅ |
| Learning Curve | Medium | Low | Low | Medium |
| Production Use | Extensive | Growing | N/A | Growing |

**Why not DVC?**: DVC is **data version control**, not workflow orchestration. It tracks file versions but doesn't schedule/run pipelines with retries, monitoring, or task dependencies like Airflow.

**Why not Prefect?**: Already in codebase but PDF explicitly allows "DVC or Airflow" — Airflow is the industry standard for production batch pipelines.

**Decision**: Airflow satisfies PDF requirement and provides production features.

---

### 4. FastAPI over Flask/Django

**Requirement**: PDF §2.5 — 5 REST endpoints with specific functionality

| Criterion | FastAPI | Flask | Django REST | Starlette |
|-----------|---------|-------|-------------|-----------|
| Async Support | ✅ Native | ❌ (threaded) | ❌ | ✅ Native |
| Auto OpenAPI Docs | ✅ Swagger/ReDoc | Manual | Manual | Manual |
| Pydantic Validation | ✅ Built-in | Manual | Serializers | Manual |
| Type Hints | ✅ First-class | Optional | Optional | ✅ |
| Performance | ⚡ High | Medium | Lower | ⚡ Highest |
| Learning Curve | Low | Low | High | Medium |

**Decision**: FastAPI provides the best developer experience for API-first development with automatic documentation — crucial for evaluation.

---

### 5. Pandas over Polars/DuckDB/Spark

| Criterion | Pandas | Polars | DuckDB | Spark |
|-----------|--------|--------|--------|-------|
| Team Familiarity | ✅ High | ❌ Low | ❌ Low | ❌ Low |
| Dataset Fit | ✅ <1M rows | ✅ | ✅ | Overkill |
| SQL Interface | ❌ | ❌ | ✅ Native | ✅ Native |
| Parquet | ✅ Native | ✅ Native | ✅ Native | ✅ Native |
| Memory | Eager | Eager/Lazy | Columnar | Distributed |

**Decision**: Pandas is sufficient for current data volumes. Migration path to Polars/DuckDB exists if scale increases.

---

## Compliance Summary

| PDF Section | Requirement | Status | Technology |
|-------------|-------------|--------|------------|
| §2.1 | Raw zone: MinIO/S3 | ✅ | MinIO |
| §2.2 | Staging: Parquet | ✅ | Apache Parquet |
| §2.2 | Curated: Parquet | ✅ | Apache Parquet |
| §2.3 | Processing: Pandas/Python | ✅ | Pandas 2.2 |
| §2.4 | Orchestration: DVC or Airflow | ✅ | Apache Airflow 2.9 |
| §2.5 | API: 5 endpoints | ✅ | FastAPI 0.109 |
| §4.1 | Validation/Error handling | ✅ | Custom + logging |

---

*All choices documented per PDF Section 5 deliverable requirements.*