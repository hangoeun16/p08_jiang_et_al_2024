"""Pull and cache WRDS structural relationships data for GSIB parent mapping.

Downloads a structural relationships parquet from Google Drive, filters
to the requested year, and caches the result in DATA_DIR.

Supports any relationship year via --year flag:
  --year 2022  (default) For WRDS replication track
  --year 2024  For FFIEC extension track

Usage
-----
    python pull_struct_rel.py                # 2022 (default)
    python pull_struct_rel.py --year 2024    # 2024 for FFIEC

Or import:
    from pull_struct_rel import load_struct_rel
    df = load_struct_rel(2022)
"""

import argparse
from pathlib import Path

import gdown
import pandas as pd

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

FILE_ID = "1LGlv9qonDPOy7WEg054wNDlM1lsPiGjt"


def _struct_rel_path(year: int) -> Path:
    """Return the cached parquet path for a given relationship year."""
    return DATA_DIR / f"struct_rel_{year}.parquet"


def pull_struct_rel(year: int = 2022) -> pd.DataFrame:
    """Download and cache WRDS structural relationships for a given year.

    Downloads the full structural relationships parquet from Google Drive,
    validates required columns, filters to reln_year == year, coerces RSSD
    identifiers to nullable integer, strips whitespace from name fields,
    and saves the result locally. If the cached file already exists, reads
    and returns it directly.

    Parameters
    ----------
    year : int
        Relationship year to filter on (e.g. 2022, 2024).

    Returns
    -------
    pd.DataFrame
        Filtered structural relationships with columns: reln_year,
        focal_rssd_id, focal_name, ultimate_rssd_id, ultimate_name.

    Raises
    ------
    ValueError
        If the downloaded parquet is missing any required columns.
    """
    out_path = _struct_rel_path(year)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        print(f"{year} structural relationships parquet already exists.")
        return pd.read_parquet(out_path)

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

    print(f"Filtering reln_year == {year} ...")
    df = df[df["reln_year"] == year].copy()

    for col in ["focal_rssd_id", "ultimate_rssd_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["focal_name", "ultimate_name"]:
        df[col] = df[col].astype(str).str.strip()

    print("Saving filtered dataset...")
    df.to_parquet(out_path, index=False)

    tmp_path.unlink(missing_ok=True)

    print(f"Saved: {out_path}")
    return df


def load_struct_rel(year: int = 2022) -> pd.DataFrame:
    """Load cached structural relationship data for a given year.

    Reads the cached parquet from DATA_DIR. If the cached file does not
    exist, falls back to pull_struct_rel() to download and build it.

    Parameters
    ----------
    year : int
        Relationship year (e.g. 2022, 2024).

    Returns
    -------
    pd.DataFrame
        Structural relationships filtered to the requested year.
    """
    out_path = _struct_rel_path(year)
    if not out_path.exists():
        return pull_struct_rel(year)
    return pd.read_parquet(out_path)


# Backwards-compatible aliases for existing code
def pull_struct_rel_2022():
    """Backwards-compatible wrapper: pull 2022 structural relationships."""
    return pull_struct_rel(2022)


def load_struct_rel_2022():
    """Backwards-compatible wrapper: load 2022 structural relationships."""
    return load_struct_rel(2022)


def main():
    parser = argparse.ArgumentParser(
        description="Pull and cache WRDS structural relationships data."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="Relationship year to filter on (default: 2022)",
    )
    args = parser.parse_args()
    df = pull_struct_rel(args.year)
    print(df.head())


if __name__ == "__main__":
    main()