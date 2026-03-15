"""Pull and cache WRDS structural relationships data for GSIB parent mapping.

This module treats a WRDS structural relationships parquet file as the raw input
and copies it into the project's DATA_DIR in a standardized location.

Expected raw input:
    - a parquet file containing columns such as:
      reln_year, focal_rssd_id, focal_name, ultimate_rssd_id, ultimate_name

Usage
-----
Cache the raw file into _data:
    python ./src/pull_struct_rel.py

Or import:
    from src.pull_struct_rel import load_struct_rel_ultimate, load_struct_rel_2022
"""
from pathlib import Path
import pandas as pd
import gdown

from settings import config


DATA_DIR = Path(config("DATA_DIR"))
STRUCT_REL_2022_PATH = DATA_DIR / "struct_rel_2022.parquet"

FILE_ID = "1LGlv9qonDPOy7WEg054wNDlM1lsPiGjt"


def pull_struct_rel_2022():
    """
    Download WRDS structural relationships parquet,
    keep only reln_year == 2022,
    and cache locally.
    """

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if STRUCT_REL_2022_PATH.exists():
        print("2022 structural relationships parquet already exists.")
        return pd.read_parquet(STRUCT_REL_2022_PATH)

    url = f"https://drive.google.com/uc?id={FILE_ID}"

    tmp_path = DATA_DIR / "struct_rel_raw.parquet"

    print("Downloading structural relationships parquet from Google Drive...")
    gdown.download(url, str(tmp_path), quiet=False)

    print("Reading parquet...")
    df = pd.read_parquet(tmp_path)

    required_cols = {
        "reln_year",
        "focal_rssd_id",
        "focal_name",
        "ultimate_rssd_id",
        "ultimate_name",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"struct_rel parquet missing required columns: {sorted(missing)}"
        )

    print("Filtering reln_year == 2022 ...")
    df = df[df["reln_year"] == 2022].copy()

    for col in ["focal_rssd_id", "ultimate_rssd_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["focal_name", "ultimate_name"]:
        df[col] = df[col].astype(str).str.strip()

    print("Saving filtered dataset...")
    df.to_parquet(STRUCT_REL_2022_PATH, index=False)

    tmp_path.unlink(missing_ok=True)

    print(f"Saved: {STRUCT_REL_2022_PATH}")

    return df


def load_struct_rel_2022():
    """Load cached structural relationship data."""
    if not STRUCT_REL_2022_PATH.exists():
        return pull_struct_rel_2022()

    return pd.read_parquet(STRUCT_REL_2022_PATH)


def main():
    df = pull_struct_rel_2022()
    print(df.head())


if __name__ == "__main__":
    main()