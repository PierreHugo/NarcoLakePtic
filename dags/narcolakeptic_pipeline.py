"""
NarcoLakePtic Pipeline DAG

Airflow DAG for orchestrating the NarcoLakePtic data lake pipeline:
1. Fetch Druglib reviews from UCI ML Repository -> MinIO raw bucket
2. Fetch openFDA drug labels from API -> MinIO raw bucket
3. Transform Raw -> Staging (cleaning, normalization, Parquet)
4. Transform Staging -> Curated (aggregations, joins, analytics-ready datasets)

Schedule: @daily (full pipeline) or @hourly (ingestion only)
"""
from datetime import datetime, timedelta
import logging
import os
import sys

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Add project root to path for imports
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import pipeline functions
from ingestion.fetch_druglib import fetch_druglib
from ingestion.fetch_openfda import fetch_openfda
from processing.raw_to_staging import process_druglib, process_openfda
from processing.staging_to_curated import build_curated

# Import validation utilities
from dags.utils.validation import (
    validate_staging_druglib,
    validate_staging_openfda,
    validate_curated_drug_catalog,
    validate_curated_review_analytics,
    validate_curated_substance_profiles,
    check_data_quality_summary,
    ValidationError,
)
from dags.utils.minio_utils import check_minio_health

log = logging.getLogger(__name__)

# Default arguments for the DAG
DEFAULT_ARGS = {
    "owner": "narcolakeptic",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# DAG definition
dag = DAG(
    dag_id="narcolakeptic_pipeline",
    default_args=DEFAULT_ARGS,
    description="NarcoLakePtic Data Lake Pipeline: Raw -> Staging -> Curated",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["narcolakeptic", "data-lake", "etl", "drug-analysis"],
    params={
        "run_ingestion": True,
        "run_staging": True,
        "run_curated": True,
        "skip_validation": False,
    },
)

# ============================================================
# TASK: Health Check
# ============================================================
def task_health_check(**context):
    """Verify MinIO and data directories are accessible."""
    log.info("Running health check...")

    # Check MinIO
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_healthy = check_minio_health(minio_endpoint)

    # Check data directories
    data_raw = os.path.exists("/opt/airflow/data/raw")
    data_staging = os.path.exists("/opt/airflow/data/staging")
    data_curated = os.path.exists("/opt/airflow/data/curated")

    # Create directories if they don't exist
    for path in ["/opt/airflow/data/raw", "/opt/airflow/data/staging", "/opt/airflow/data/curated"]:
        os.makedirs(path, exist_ok=True)

    if not minio_healthy:
        raise RuntimeError("MinIO health check failed")

    log.info("Health check passed: MinIO OK, data directories accessible")
    return {"minio": minio_healthy, "data_dirs": True}


health_check = PythonOperator(
    task_id="health_check",
    python_callable=task_health_check,
    dag=dag,
)

# ============================================================
# TASK: Fetch Druglib
# ============================================================
def task_fetch_druglib(**context):
    """Fetch Druglib dataset from UCI and push to MinIO raw bucket."""
    log.info("Starting Druglib ingestion...")
    row_count = fetch_druglib()

    # Push row count to XCom for downstream tasks
    context["ti"].xcom_push(key="druglib_rows", value=row_count)
    log.info(f"Druglib ingestion complete: {row_count} rows")
    return row_count


fetch_druglib_task = PythonOperator(
    task_id="fetch_druglib",
    python_callable=task_fetch_druglib,
    dag=dag,
)

# ============================================================
# TASK: Fetch openFDA
# ============================================================
def task_fetch_openfda(**context):
    """Fetch openFDA drug labels from API and push to MinIO raw bucket."""
    log.info("Starting openFDA ingestion...")
    row_count = fetch_openfda()

    context["ti"].xcom_push(key="openfda_rows", value=row_count)
    log.info(f"openFDA ingestion complete: {row_count} rows")
    return row_count


fetch_openfda_task = PythonOperator(
    task_id="fetch_openfda",
    python_callable=task_fetch_openfda,
    dag=dag,
)

# ============================================================
# TASK: Raw to Staging - Druglib
# ============================================================
def task_raw_to_staging_druglib(**context):
    """Transform Druglib raw CSV to staging Parquet with validation."""
    log.info("Transforming Druglib: Raw -> Staging...")

    input_path = "/opt/airflow/data/raw/druglib_reviews_static.csv"
    output_path = "/opt/airflow/data/staging/druglib.parquet"

    process_druglib(input_path, output_path)

    # Validate output
    if not context["params"].get("skip_validation", False):
        df = pd.read_parquet(output_path)
        errors = validate_staging_druglib(df)
        if errors:
            raise ValidationError(f"Druglib staging validation failed: {errors}")

        summary = check_data_quality_summary(df, "druglib")
        log.info(f"Druglib staging quality: {summary}")
        context["ti"].xcom_push(key="druglib_staging_summary", value=summary)

    log.info("Druglib Raw -> Staging complete")


raw_to_staging_druglib_task = PythonOperator(
    task_id="raw_to_staging_druglib",
    python_callable=task_raw_to_staging_druglib,
    dag=dag,
)

# ============================================================
# TASK: Raw to Staging - openFDA
# ============================================================
def task_raw_to_staging_openfda(**context):
    """Transform openFDA raw CSV to staging Parquet with validation."""
    log.info("Transforming openFDA: Raw -> Staging...")

    input_path = "/opt/airflow/data/raw/openfda_drug_labels_api.csv"
    output_path = "/opt/airflow/data/staging/openfda.parquet"

    process_openfda(input_path, output_path)

    # Validate output
    if not context["params"].get("skip_validation", False):
        df = pd.read_parquet(output_path)
        errors = validate_staging_openfda(df)
        if errors:
            raise ValidationError(f"openFDA staging validation failed: {errors}")

        summary = check_data_quality_summary(df, "openfda")
        log.info(f"openFDA staging quality: {summary}")
        context["ti"].xcom_push(key="openfda_staging_summary", value=summary)

    log.info("openFDA Raw -> Staging complete")


raw_to_staging_openfda_task = PythonOperator(
    task_id="raw_to_staging_openfda",
    python_callable=task_raw_to_staging_openfda,
    dag=dag,
)

# ============================================================
# TASK: Staging to Curated
# ============================================================
def task_staging_to_curated(**context):
    """Build curated datasets from staging data with validation."""
    log.info("Building curated datasets: Staging -> Curated...")

    build_curated()

    # Validate curated outputs
    if not context["params"].get("skip_validation", False):
        curated_dir = "/opt/airflow/data/curated"

        # Validate drug_catalog
        catalog_path = f"{curated_dir}/drug_catalog.parquet"
        if os.path.exists(catalog_path):
            df = pd.read_parquet(catalog_path)
            errors = validate_curated_drug_catalog(df)
            if errors:
                raise ValidationError(f"Drug catalog validation failed: {errors}")
            summary = check_data_quality_summary(df, "drug_catalog")
            log.info(f"Drug catalog quality: {summary}")
            context["ti"].xcom_push(key="drug_catalog_summary", value=summary)

        # Validate review_analytics
        reviews_path = f"{curated_dir}/review_analytics.parquet"
        if os.path.exists(reviews_path):
            df = pd.read_parquet(reviews_path)
            errors = validate_curated_review_analytics(df)
            if errors:
                raise ValidationError(f"Review analytics validation failed: {errors}")
            summary = check_data_quality_summary(df, "review_analytics")
            log.info(f"Review analytics quality: {summary}")
            context["ti"].xcom_push(key="review_analytics_summary", value=summary)

        # Validate substance_profiles
        profiles_path = f"{curated_dir}/substance_profiles.parquet"
        if os.path.exists(profiles_path):
            df = pd.read_parquet(profiles_path)
            errors = validate_curated_substance_profiles(df)
            if errors:
                raise ValidationError(f"Substance profiles validation failed: {errors}")
            summary = check_data_quality_summary(df, "substance_profiles")
            log.info(f"Substance profiles quality: {summary}")
            context["ti"].xcom_push(key="substance_profiles_summary", value=summary)

    log.info("Staging -> Curated complete")


staging_to_curated_task = PythonOperator(
    task_id="staging_to_curated",
    python_callable=task_staging_to_curated,
    dag=dag,
)

# ============================================================
# TASK: Pipeline Summary
# ============================================================
def task_pipeline_summary(**context):
    """Generate and log pipeline execution summary."""
    log.info("=== Pipeline Execution Summary ===")

    # Collect XCom values from upstream tasks
    ti = context["ti"]

    druglib_rows = ti.xcom_pull(key="druglib_rows", task_ids="fetch_druglib") or 0
    openfda_rows = ti.xcom_pull(key="openfda_rows", task_ids="fetch_openfda") or 0

    druglib_staging = ti.xcom_pull(key="druglib_staging_summary", task_ids="raw_to_staging_druglib") or {}
    openfda_staging = ti.xcom_pull(key="openfda_staging_summary", task_ids="raw_to_staging_openfda") or {}

    catalog_summary = ti.xcom_pull(key="drug_catalog_summary", task_ids="staging_to_curated") or {}
    reviews_summary = ti.xcom_pull(key="review_analytics_summary", task_ids="staging_to_curated") or {}
    profiles_summary = ti.xcom_pull(key="substance_profiles_summary", task_ids="staging_to_curated") or {}

    log.info(f"Raw ingestion: Druglib={druglib_rows}, openFDA={openfda_rows}")
    log.info(f"Staging: Druglib={druglib_staging.get('rows', 'N/A')}, openFDA={openfda_staging.get('rows', 'N/A')}")
    log.info(
        f"Curated: Drug Catalog={catalog_summary.get('rows', 'N/A')}, "
        f"Review Analytics={reviews_summary.get('rows', 'N/A')}, "
        f"Substance Profiles={profiles_summary.get('rows', 'N/A')}"
    )

    run_id = context["run_id"]
    log.info(f"Pipeline run {run_id} completed successfully")

    return {
        "run_id": run_id,
        "raw_rows": {"druglib": druglib_rows, "openfda": openfda_rows},
        "staging": {"druglib": druglib_staging, "openfda": openfda_staging},
        "curated": {
            "drug_catalog": catalog_summary,
            "review_analytics": reviews_summary,
            "substance_profiles": profiles_summary,
        },
    }


pipeline_summary = PythonOperator(
    task_id="pipeline_summary",
    python_callable=task_pipeline_summary,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================
# Health check runs first
health_check >> [fetch_druglib_task, fetch_openfda_task]

# Ingestion tasks run in parallel, then staging tasks
fetch_druglib_task >> raw_to_staging_druglib_task
fetch_openfda_task >> raw_to_staging_openfda_task

# Staging tasks run in parallel, then curated
[raw_to_staging_druglib_task, raw_to_staging_openfda_task] >> staging_to_curated_task

# Summary runs last
staging_to_curated_task >> pipeline_summary