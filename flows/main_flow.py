"""
Prefect main flow - orchestration complète du pipeline
Raw -> Staging -> Curated
"""
from prefect import flow, task
from ingestion.fetch_druglib import fetch_druglib
from ingestion.fetch_openfda import fetch_openfda
from processing.raw_to_staging import process_druglib, process_openfda
from processing.staging_to_curated import build_curated

@task
def task_fetch_druglib():
    fetch_druglib()

@task
def task_fetch_openfda():
    fetch_openfda()

@task
def task_raw_to_staging():
    process_druglib("data/raw/druglib_reviews_static.csv", "data/staging/druglib.parquet")
    process_openfda("data/raw/openfda_drug_labels_api.csv", "data/staging/openfda.parquet")

@task
def task_staging_to_curated():
    build_curated()

@flow(name="NarcoLakePtic Pipeline")
def main_flow():
    task_fetch_druglib()
    task_fetch_openfda()
    task_raw_to_staging()
    task_staging_to_curated()

if __name__ == "__main__":
    main_flow()