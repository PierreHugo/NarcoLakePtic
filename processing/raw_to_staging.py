"""
Transformation Raw -> Staging
- Nettoyage des données brutes
- Typage des colonnes
- Suppression des doublons
"""
import pandas as pd

def process_unodc(input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    # TODO: nettoyage spécifique UNODC
    df = df.dropna()
    df.to_parquet(output_path, index=False)
    print(f"Staging done: {len(df)} rows")

if __name__ == "__main__":
    process_unodc("data/raw/unodc.csv", "data/staging/unodc.parquet")
