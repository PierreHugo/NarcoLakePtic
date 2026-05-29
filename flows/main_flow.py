"""
Prefect main flow - orchestration complète du pipeline
Raw -> Staging -> Curated
"""
from prefect import flow, task
from ingestion.fetch_unodc import fetch_unodc
from ingestion.fetch_emcdda import fetch_emcdda
from processing.raw_to_staging import process_unodc
from processing.staging_to_curated import build_curated

@task
def task_fetch_unodc():
    fetch_unodc()

@task
def task_fetch_emcdda():
    fetch_emcdda()

@task
def task_raw_to_staging():
    process_unodc("data/raw/unodc.csv", "data/staging/unodc.parquet")

@task
def task_staging_to_curated():
    build_curated()

@flow(name="NarcoLakePtic Pipeline")
def main_flow():
    task_fetch_unodc()
    task_fetch_emcdda()
    task_raw_to_staging()
    task_staging_to_curated()

if __name__ == "__main__":
    main_flow()
