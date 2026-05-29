"""
Transformation Staging -> Curated
- Agrégations
- Jointures entre sources
- Données prêtes pour l'analyse
"""
import pandas as pd

def build_curated():
    # TODO: charger depuis MinIO staging
    df = pd.read_parquet("data/staging/unodc.parquet")
    # TODO: transformations métier
    df.to_parquet("data/curated/final.parquet", index=False)
    print("Curated done.")

if __name__ == "__main__":
    build_curated()
