"""
Transformation Staging -> Curated
- Agrégations
- Jointures entre sources
- Données prêtes pour l'analyse
"""
import logging
from pathlib import Path

import pandas as pd

from dags.utils.validation import (
    validate_curated_drug_catalog,
    validate_curated_review_analytics,
    validate_curated_substance_profiles,
    check_data_quality_summary,
    ValidationError,
)

# Configure logging
log = logging.getLogger(__name__)


def load_staging(validate: bool = True) -> dict:
    """
    Charge les données en staging.

    Args:
        validate: Whether to validate loaded data

    Returns:
        Dictionary with 'druglib' and 'openfda' DataFrames
    """
    datasets = {}

    # Druglib reviews
    druglib_path = Path("data/staging/druglib.parquet")
    if druglib_path.exists():
        datasets['druglib'] = pd.read_parquet(druglib_path)
        log.info(f"Loaded druglib: {len(datasets['druglib'])} rows")

        if validate:
            from dags.utils.validation import validate_staging_druglib
            errors = validate_staging_druglib(datasets['druglib'])
            if errors:
                raise ValidationError(f"Staging druglib validation failed: {errors}")
    else:
        log.warning("Warning: druglib.parquet not found in staging")
        datasets['druglib'] = pd.DataFrame()

    # openFDA drug labels
    openfda_path = Path("data/staging/openfda.parquet")
    if openfda_path.exists():
        datasets['openfda'] = pd.read_parquet(openfda_path)
        log.info(f"Loaded openfda: {len(datasets['openfda'])} rows")

        if validate:
            from dags.utils.validation import validate_staging_openfda
            errors = validate_staging_openfda(datasets['openfda'])
            if errors:
                raise ValidationError(f"Staging openfda validation failed: {errors}")
    else:
        log.warning("Warning: openfda.parquet not found in staging")
        datasets['openfda'] = pd.DataFrame()

    return datasets


def build_drug_catalog(openfda_df: pd.DataFrame, validate: bool = True) -> pd.DataFrame:
    """
    Construit un catalogue de médicaments uniques depuis openFDA.

    Args:
        openfda_df: openFDA staging DataFrame
        validate: Whether to validate output

    Returns:
        Drug catalog DataFrame
    """
    if openfda_df.empty:
        log.warning("openFDA DataFrame is empty, returning empty catalog")
        return pd.DataFrame()

    # Clés pour dédupliquer : substance_name + brand_name + manufacturer
    catalog_cols = [
        'spl_id', 'brand_name', 'generic_name', 'manufacturer_name',
        'substance_name', 'product_type', 'route', 'effective_time'
    ]

    catalog = openfda_df[catalog_cols].drop_duplicates(
        subset=['substance_name', 'brand_name', 'manufacturer_name']
    )

    # Ajouter des métriques de complétude
    catalog['has_indications'] = openfda_df['indications_and_usage'].notna() & (openfda_df['indications_and_usage'] != '')
    catalog['has_warnings'] = openfda_df['warnings'].notna() & (openfda_df['warnings'] != '')
    catalog['has_adverse_reactions'] = openfda_df['adverse_reactions'].notna() & (openfda_df['adverse_reactions'] != '')
    catalog['has_interactions'] = openfda_df['drug_interactions'].notna() & (openfda_df['drug_interactions'] != '')

    # Validation
    if validate:
        log.info("Validating drug catalog...")
        errors = validate_curated_drug_catalog(catalog)
        if errors:
            raise ValidationError(f"Drug catalog validation failed: {errors}")
        summary = check_data_quality_summary(catalog, "drug_catalog")
        log.info(f"Drug catalog quality: {summary}")

    return catalog


def build_review_analytics(druglib_df: pd.DataFrame, validate: bool = True) -> pd.DataFrame:
    """
    Analyse les avis patients depuis Druglib.

    Args:
        druglib_df: Druglib staging DataFrame
        validate: Whether to validate output

    Returns:
        Review analytics DataFrame
    """
    if druglib_df.empty:
        log.warning("Druglib DataFrame is empty, returning empty analytics")
        return pd.DataFrame()

    # Normaliser les noms de colonnes
    druglib_df = druglib_df.rename(columns={
        'urlDrugName': 'drug_name',
        'condition': 'condition_treated'
    })

    # Agrégations par médicament
    analytics = druglib_df.groupby('drug_name').agg(
        total_reviews=('rating', 'count'),
        avg_rating=('rating', 'mean'),
        avg_effectiveness=('effectiveness', 'mean'),
        avg_side_effects=('sideEffects', 'mean'),
        conditions=('condition_treated', lambda x: ' | '.join(x.dropna().unique()[:10])),
        sample_reviews=('commentsReview', lambda x: ' ||| '.join(x.dropna().astype(str).head(3)))
    ).reset_index()

    analytics['avg_rating'] = analytics['avg_rating'].round(2)
    analytics['avg_effectiveness'] = analytics['avg_effectiveness'].round(2)
    analytics['avg_side_effects'] = analytics['avg_side_effects'].round(2)

    # Validation
    if validate:
        log.info("Validating review analytics...")
        errors = validate_curated_review_analytics(analytics)
        if errors:
            raise ValidationError(f"Review analytics validation failed: {errors}")
        summary = check_data_quality_summary(analytics, "review_analytics")
        log.info(f"Review analytics quality: {summary}")

    return analytics


def build_substance_profile(openfda_df: pd.DataFrame, druglib_df: pd.DataFrame, validate: bool = True) -> pd.DataFrame:
    """
    Crée un profil enrichi par substance active.

    Args:
        openfda_df: openFDA staging DataFrame
        druglib_df: Druglib staging DataFrame
        validate: Whether to validate output

    Returns:
        Substance profile DataFrame
    """
    if openfda_df.empty:
        log.warning("openFDA DataFrame is empty, returning empty profiles")
        return pd.DataFrame()

    # Profils openFDA par substance
    substance_profile = openfda_df.groupby('substance_name').agg(
        num_products=('spl_id', 'nunique'),
        num_manufacturers=('manufacturer_name', 'nunique'),
        product_types=('product_type', lambda x: ' | '.join(x.dropna().unique())),
        routes=('route', lambda x: ' | '.join(x.dropna().unique())),
        first_approval=('effective_time', 'min'),
        latest_update=('effective_time', 'max'),
        has_indications=('indications_and_usage', lambda x: (x.notna() & (x != '')).any()),
        has_warnings=('warnings', lambda x: (x.notna() & (x != '')).any()),
        has_interactions=('drug_interactions', lambda x: (x.notna() & (x != '')).any()),
        has_adverse_reactions=('adverse_reactions', lambda x: (x.notna() & (x != '')).any()),
    ).reset_index()

    # Enrichir avec les reviews Druglib si disponible
    if not druglib_df.empty:
        # Mapper drug_name vers substance_name (approximatif)
        # On joint sur le nom générique
        druglib_drugs = build_review_analytics(druglib_df, validate=False)
        druglib_drugs = druglib_drugs.rename(columns={'drug_name': 'generic_name'})

        # Renommer substance_name vers generic_name pour la jointure
        substance_profile = substance_profile.rename(columns={'substance_name': 'generic_name'})

        substance_profile = substance_profile.merge(
            druglib_drugs[['generic_name', 'avg_rating', 'total_reviews']],
            on='generic_name',
            how='left'
        )

    # Validation
    if validate:
        log.info("Validating substance profiles...")
        errors = validate_curated_substance_profiles(substance_profile)
        if errors:
            raise ValidationError(f"Substance profiles validation failed: {errors}")
        summary = check_data_quality_summary(substance_profile, "substance_profiles")
        log.info(f"Substance profiles quality: {summary}")

    return substance_profile


def build_combined_analysis(openfda_df: pd.DataFrame, druglib_df: pd.DataFrame) -> pd.DataFrame:
    """
    Dataset combiné pour analyse croisée (si les deux sources existent).

    Args:
        openfda_df: openFDA staging DataFrame
        druglib_df: Druglib staging DataFrame

    Returns:
        Combined analysis DataFrame
    """
    if openfda_df.empty or druglib_df.empty:
        log.warning("One or both DataFrames empty, skipping combined analysis")
        return pd.DataFrame()

    # Jointure approximative sur nom générique
    combined = openfda_df.merge(
        druglib_df.rename(columns={'urlDrugName': 'generic_name'}),
        on='generic_name',
        how='inner',
        suffixes=('_fda', '_review')
    )

    if not combined.empty:
        log.info(f"Created combined_analysis: {len(combined)} matched records")

    return combined


def build_curated(validate: bool = True) -> dict:
    """
    Pipeline principal Staging -> Curated.

    Args:
        validate: Whether to run validation checks

    Returns:
        Dictionary with output row counts
    """
    Path("data/curated").mkdir(parents=True, exist_ok=True)

    datasets = load_staging(validate=validate)
    openfda_df = datasets['openfda']
    druglib_df = datasets['druglib']

    outputs = {}

    # 1. Catalogue de médicaments (openFDA)
    catalog = build_drug_catalog(openfda_df, validate=validate)
    if not catalog.empty:
        catalog.to_parquet("data/curated/drug_catalog.parquet", index=False)
        outputs['drug_catalog'] = len(catalog)
        log.info(f"Created drug_catalog: {len(catalog)} drugs")

    # 2. Analytics reviews patients (Druglib)
    reviews = build_review_analytics(druglib_df, validate=validate)
    if not reviews.empty:
        reviews.to_parquet("data/curated/review_analytics.parquet", index=False)
        outputs['review_analytics'] = len(reviews)
        log.info(f"Created review_analytics: {len(reviews)} drugs")

    # 3. Profils de substances enrichis
    profiles = build_substance_profile(openfda_df, druglib_df, validate=validate)
    if not profiles.empty:
        profiles.to_parquet("data/curated/substance_profiles.parquet", index=False)
        outputs['substance_profiles'] = len(profiles)
        log.info(f"Created substance_profiles: {len(profiles)} substances")

    # 4. Dataset combiné pour analyse croisée (si les deux sources existent)
    combined = build_combined_analysis(openfda_df, druglib_df)
    if not combined.empty:
        combined.to_parquet("data/curated/combined_analysis.parquet", index=False)
        outputs['combined_analysis'] = len(combined)
        log.info(f"Created combined_analysis: {len(combined)} matched records")

    # Résumé
    log.info("\n=== Curated Layer Summary ===")
    for name, count in outputs.items():
        log.info(f"  {name}: {count} rows")

    if not outputs:
        log.warning("No data processed - check staging inputs")

    return outputs


def main():
    """Main entry point for direct script execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Staging to Curated transformation")
    parser.add_argument("--no-validate", action="store_true", help="Skip validation")
    args = parser.parse_args()

    build_curated(validate=not args.no_validate)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S"
    )
    main()