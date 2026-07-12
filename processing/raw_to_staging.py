"""
Transformation Raw -> Staging
- Nettoyage des données brutes
- Typage des colonnes
- Suppression des doublons
- Validation des données
"""
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Add project root to path for validation imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dags.utils.validation import (
    validate_staging_druglib,
    validate_staging_openfda,
    check_data_quality_summary,
    ValidationError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def process_druglib(input_path: str, output_path: str, validate: bool = True) -> pd.DataFrame:
    """
    Process Druglib.com patient reviews dataset.

    Args:
        input_path: Path to raw CSV file
        output_path: Path to output Parquet file
        validate: Whether to run validation checks

    Returns:
        Processed DataFrame

    Raises:
        ValidationError: If validation fails
        FileNotFoundError: If input file doesn't exist
    """
    log.info(f"Processing Druglib: {input_path} -> {output_path}")

    # Check input exists
    if not Path(input_path).exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read raw data
    df = pd.read_csv(input_path)
    log.info(f"Loaded raw Druglib: {len(df)} rows, {len(df.columns)} columns")

    # Nettoyage spécifique Druglib
    # Colonnes attendues: reviewID, urlDrugName, rating, effectiveness, sideEffects, condition, benefitsReview, sideEffectsReview, commentsReview

    # Nettoyer les noms de drogues (enlever URLs, normaliser)
    df['urlDrugName'] = df['urlDrugName'].str.replace(r'https?://\S+', '', regex=True).str.strip()

    # Convertir rating en numérique
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['effectiveness'] = pd.to_numeric(df['effectiveness'], errors='coerce')
    df['sideEffects'] = pd.to_numeric(df['sideEffects'], errors='coerce')

    # Supprimer les lignes sans drug name ou rating
    initial_rows = len(df)
    df = df.dropna(subset=['urlDrugName', 'rating'])
    log.info(f"Dropped {initial_rows - len(df)} rows with missing drug name or rating")

    # Supprimer les doublons basé sur reviewID
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['reviewID'], keep='first')
    log.info(f"Dropped {initial_rows - len(df)} duplicate reviewIDs")

    # Nettoyer les champs texte
    text_cols = ['benefitsReview', 'sideEffectsReview', 'commentsReview', 'condition']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', '')

    # Rename column for consistency
    df = df.rename(columns={'urlDrugName': 'drug_name', 'condition': 'condition_treated'})

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Validation
    if validate:
        log.info("Running Druglib staging validation...")
        errors = validate_staging_druglib(df)
        if errors:
            raise ValidationError(f"Druglib validation failed: {errors}")
        summary = check_data_quality_summary(df, "druglib_staging")
        log.info(f"Quality summary: {summary}")

    # Write to Parquet
    df.to_parquet(output_path, index=False)
    file_size = Path(output_path).stat().st_size / (1024 * 1024)
    log.info(f"Druglib staging done: {len(df)} rows -> {output_path} ({file_size:.2f} MB)")

    return df


def process_openfda(input_path: str, output_path: str, validate: bool = True) -> pd.DataFrame:
    """
    Process openFDA drug labels dataset.

    Args:
        input_path: Path to raw CSV file
        output_path: Path to output Parquet file
        validate: Whether to run validation checks

    Returns:
        Processed DataFrame

    Raises:
        ValidationError: If validation fails
        FileNotFoundError: If input file doesn't exist
    """
    log.info(f"Processing openFDA: {input_path} -> {output_path}")

    # Check input exists
    if not Path(input_path).exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Read raw data
    df = pd.read_csv(input_path, low_memory=False)
    log.info(f"Loaded raw openFDA: {len(df)} rows, {len(df.columns)} columns")

    # Colonnes: spl_id, brand_name, generic_name, manufacturer_name, substance_name,
    # product_type, route, effective_time, indications_and_usage, warnings,
    # dosage_and_administration, active_ingredient, do_not_use, contraindications,
    # drug_interactions, adverse_reactions

    # Convertir effective_time en datetime
    if 'effective_time' in df.columns:
        df['effective_time'] = pd.to_datetime(df['effective_time'], format='%Y%m%d', errors='coerce')

    # Supprimer les lignes sans identifiant
    initial_rows = len(df)
    df = df.dropna(subset=['spl_id'])
    log.info(f"Dropped {initial_rows - len(df)} rows with missing spl_id")

    # Supprimer les doublons basé sur spl_id
    initial_rows = len(df)
    df = df.drop_duplicates(subset=['spl_id'], keep='first')
    log.info(f"Dropped {initial_rows - len(df)} duplicate spl_ids")

    # Nettoyer les champs texte (enlever caractères de contrôle, normaliser espaces)
    text_cols = ['brand_name', 'generic_name', 'manufacturer_name', 'substance_name',
                 'product_type', 'route', 'indications_and_usage', 'warnings',
                 'dosage_and_administration', 'active_ingredient', 'do_not_use',
                 'contraindications', 'drug_interactions', 'adverse_reactions']

    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(r'[\x00-\x1f\x7f-\x9f]', '', regex=True)
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
            df[col] = df[col].replace('nan', '')

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Validation
    if validate:
        log.info("Running openFDA staging validation...")
        errors = validate_staging_openfda(df)
        if errors:
            raise ValidationError(f"openFDA validation failed: {errors}")
        summary = check_data_quality_summary(df, "openfda_staging")
        log.info(f"Quality summary: {summary}")

    # Write to Parquet
    df.to_parquet(output_path, index=False)
    file_size = Path(output_path).stat().st_size / (1024 * 1024)
    log.info(f"openFDA staging done: {len(df)} rows -> {output_path} ({file_size:.2f} MB)")

    return df


def main():
    """Main entry point for direct script execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Raw to Staging transformation")
    parser.add_argument("--no-validate", action="store_true", help="Skip validation")
    parser.add_argument("--druglib-only", action="store_true", help="Process only Druglib")
    parser.add_argument("--openfda-only", action="store_true", help="Process only openFDA")
    args = parser.parse_args()

    validate = not args.no_validate

    if not args.openfda_only:
        process_druglib(
            "data/raw/druglib_reviews_static.csv",
            "data/staging/druglib.parquet",
            validate=validate,
        )

    if not args.druglib_only:
        process_openfda(
            "data/raw/openfda_drug_labels_api.csv",
            "data/staging/openfda.parquet",
            validate=validate,
        )


if __name__ == "__main__":
    main()