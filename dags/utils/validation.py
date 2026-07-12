"""
Data validation utilities for NarcoLakePtic Airflow DAGs.
Provides schema validation, null checks, range checks, and referential integrity checks.
"""
import pandas as pd
import logging
from typing import Dict, List, Optional, Any

log = logging.getLogger(__name__)

# Expected schemas for validation
DRUGLIB_STAGING_SCHEMA = {
    "required_columns": ["reviewID", "drug_name", "rating", "condition_treated"],
    "numeric_columns": {"rating": (1, 5), "effectiveness": (1, 5), "sideEffects": (1, 5)},
    "text_columns": ["drug_name", "condition_treated", "benefitsReview", "sideEffectsReview", "commentsReview"],
    "no_null_keys": ["reviewID", "drug_name"],
    "dtype_expectations": {"rating": "float64", "effectiveness": "float64", "sideEffects": "float64"},
}

OPENFDA_STAGING_SCHEMA = {
    "required_columns": ["spl_id", "brand_name", "generic_name", "manufacturer_name", "substance_name"],
    "no_null_keys": ["spl_id", "substance_name"],
    "text_columns": [
        "brand_name", "generic_name", "manufacturer_name", "substance_name",
        "product_type", "route", "effective_time",
        "indications_and_usage", "warnings", "dosage_and_administration",
        "active_ingredient", "do_not_use", "contraindications",
        "drug_interactions", "adverse_reactions",
    ],
}

CURATED_DRUG_CATALOG_SCHEMA = {
    "required_columns": ["spl_id", "brand_name", "generic_name", "manufacturer_name", "substance_name"],
    "no_null_keys": [],
    "unique_keys": ["substance_name", "brand_name", "manufacturer_name"],
}

CURATED_REVIEW_ANALYTICS_SCHEMA = {
    "required_columns": ["drug_name", "total_reviews", "avg_rating"],
    "no_null_keys": ["drug_name"],
    "numeric_columns": {"avg_rating": (1, 5), "avg_effectiveness": (1, 5), "avg_side_effects": (1, 5)},
    "unique_keys": ["drug_name"],
}

CURATED_SUBSTANCE_PROFILES_SCHEMA = {
    "required_columns": ["generic_name", "num_products", "num_manufacturers"],
    "no_null_keys": ["generic_name"],
    "unique_keys": ["generic_name"],
}


class ValidationError(Exception):
    """Custom exception for validation failures."""
    pass


def validate_schema(df: pd.DataFrame, schema: Dict[str, Any], layer: str, dataset: str) -> List[str]:
    """
    Validate a DataFrame against a schema definition.

    Args:
        df: DataFrame to validate
        schema: Schema definition dict
        layer: Layer name (staging/curated)
        dataset: Dataset name for logging

    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []

    if df.empty:
        errors.append(f"{layer}/{dataset}: DataFrame is empty")
        return errors

    # Check required columns
    required = schema.get("required_columns", [])
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        errors.append(f"{layer}/{dataset}: Missing required columns: {missing_cols}")

    # Check no-null keys
    no_null_keys = schema.get("no_null_keys", [])
    for col in no_null_keys:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                errors.append(f"{layer}/{dataset}: Column '{col}' has {null_count} null values (no-null key)")

    # Check numeric ranges
    numeric_ranges = schema.get("numeric_columns", {})
    for col, (min_val, max_val) in numeric_ranges.items():
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
            if len(out_of_range) > 0:
                errors.append(
                    f"{layer}/{dataset}: Column '{col}' has {len(out_of_range)} values "
                    f"outside range [{min_val}, {max_val}]"
                )

    # Check unique keys
    unique_keys = schema.get("unique_keys", [])
    for key in unique_keys:
        if key in df.columns:
            dup_count = df.duplicated(subset=[key]).sum()
            if dup_count > 0:
                errors.append(f"{layer}/{dataset}: Column '{key}' has {dup_count} duplicate values (expected unique)")

    return errors


def validate_staging_druglib(df: pd.DataFrame) -> List[str]:
    """Validate Druglib staging data."""
    return validate_schema(df, DRUGLIB_STAGING_SCHEMA, "staging", "druglib")


def validate_staging_openfda(df: pd.DataFrame) -> List[str]:
    """Validate openFDA staging data."""
    return validate_schema(df, OPENFDA_STAGING_SCHEMA, "staging", "openfda")


def validate_curated_drug_catalog(df: pd.DataFrame) -> List[str]:
    """Validate curated drug catalog."""
    return validate_schema(df, CURATED_DRUG_CATALOG_SCHEMA, "curated", "drug_catalog")


def validate_curated_review_analytics(df: pd.DataFrame) -> List[str]:
    """Validate curated review analytics."""
    return validate_schema(df, CURATED_REVIEW_ANALYTICS_SCHEMA, "curated", "review_analytics")


def validate_curated_substance_profiles(df: pd.DataFrame) -> List[str]:
    """Validate curated substance profiles."""
    return validate_schema(df, CURATED_SUBSTANCE_PROFILES_SCHEMA, "curated", "substance_profiles")


def validate_referential_integrity(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    left_key: str,
    right_key: str,
    join_name: str,
) -> List[str]:
    """
    Validate referential integrity between two DataFrames on a join key.

    Args:
        left_df: Left DataFrame (e.g., curated)
        right_df: Right DataFrame (e.g., staging)
        left_key: Join key in left DataFrame
        right_key: Join key in right DataFrame
        join_name: Descriptive name for logging

    Returns:
        List of validation error messages
    """
    errors = []

    if left_df.empty or right_df.empty:
        errors.append(f"{join_name}: One or both DataFrames empty")
        return errors

    if left_key not in left_df.columns:
        errors.append(f"{join_name}: Left key '{left_key}' not in left DataFrame")
        return errors

    if right_key not in right_df.columns:
        errors.append(f"{join_name}: Right key '{right_key}' not in right DataFrame")
        return errors

    left_keys = set(left_df[left_key].dropna().unique())
    right_keys = set(right_df[right_key].dropna().unique())

    orphaned = left_keys - right_keys
    if orphaned:
        errors.append(
            f"{join_name}: {len(orphaned)} keys in left have no match in right "
            f"(e.g., {list(orphaned)[:5]})"
        )

    return errors


def check_data_quality_summary(df: pd.DataFrame, name: str) -> Dict[str, Any]:
    """
    Generate a data quality summary for a DataFrame.

    Args:
        df: DataFrame to summarize
        name: Dataset name

    Returns:
        Dictionary with quality metrics
    """
    if df.empty:
        return {"name": name, "rows": 0, "columns": 0, "empty": True}

    total_cells = df.size
    null_cells = df.isna().sum().sum()

    return {
        "name": name,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "empty": False,
        "null_count": int(null_cells),
        "null_percentage": round(null_cells / total_cells * 100, 2) if total_cells > 0 else 0,
        "duplicate_rows": int(df.duplicated().sum()),
        "memory_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        "column_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
    }