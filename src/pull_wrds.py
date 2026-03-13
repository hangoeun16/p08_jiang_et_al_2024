"""Pull and cache WRDS Call Report data needed to replicate Jiang et al. (2024).

Fetches four Call Report data series from WRDS via SQL:
  - RCON_Series_1: domestic balance sheet items (RMBS, first-lien mortgages,
    uninsured deposits, insured deposits) from wrds_call_rcon_1
  - RCON_Series_2: domestic securities (treasuries/other), other loans,
    total assets from wrds_call_rcon_2
  - RCFD_Series_1: domestic+foreign balance sheet items (RMBS, first-lien
    mortgages, other loans, cash) from wrds_call_rcfd_1
  - RCFD_Series_2: domestic+foreign treasury securities, total assets
    from wrds_call_rcfd_2

Each pull function saves results as parquet to DATA_DIR. Corresponding
load_* functions read from those cached parquet files.

Usage
-----
Run directly to pull and cache all series:
    python pull_wrds.py

Or import and use individual pull/load functions:
    from pull_wrds import load_rcon_series_1
"""

import pandas as pd
import numpy as np
import wrds
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


# ---------------------------------------------------------------------------
# Pull functions — connect to WRDS and return a DataFrame
# ---------------------------------------------------------------------------


def pull_rcon_series_1(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """Pull domestic balance sheet series 1 from WRDS Call Reports.

    Fetches RMBS by maturity (rcona555-560), first-lien residential mortgages
    by maturity (rcona564-569), uninsured deposits (rcon5597), and insured
    deposits (rconf049, rconf045) from wrds_call_rcon_1.

    Note: If WRDS does not expose rcona555-560 or rcona564-569 in the
    wrds_call_rcon_1 view, the query will raise a column-not-found error.
    In that case, use the RCFD variants from pull_rcfd_series_1 instead.

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.
    wrds_username : str
        WRDS account username.

    Returns
    -------
    pd.DataFrame
    """
    # Note: rcona556, rcona558-560 are in wrds_call_rcon_2, not rcon_1.
    # Only rcona555 (<3m) and rcona557 (1y-3y) exist in rcon_1.
    sql = f"""
        SELECT
            b.rssd9001, b.rssd9017, b.rssd9999,
            b.rcona555, b.rcona557,
            b.rcona564, b.rcona565, b.rcona566, b.rcona567, b.rcona568, b.rcona569,
            b.rcon5597,
            b.rconf049, b.rconf045
        FROM bank.wrds_call_rcon_1 AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df


def pull_rcon_series_2(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """Pull domestic balance sheet series 2 from WRDS Call Reports.

    Fetches treasury/other securities by maturity (rcona549-554), other
    (non-first-lien) loans by maturity (rcona570-575), and total assets
    (rcon2170) from wrds_call_rcon_2.

    Parameters
    ----------
    start_date : str or datetime
    end_date : str or datetime
    wrds_username : str

    Returns
    -------
    pd.DataFrame
    """
    # Note: rcona556 (3m-1y), rcona558-560 (3y-5y, 5y-15y, >15y) RMBS buckets
    # are stored in rcon_2, not rcon_1. Include them here alongside treasuries.
    sql = f"""
        SELECT
            b.rssd9001, b.rssd9017, b.rssd9999,
            b.rcona549, b.rcona550, b.rcona551, b.rcona552, b.rcona553, b.rcona554,
            b.rcona556, b.rcona558, b.rcona559, b.rcona560,
            b.rcona570, b.rcona571, b.rcona572, b.rcona573, b.rcona574, b.rcona575,
            b.rcon2170
        FROM bank.wrds_call_rcon_2 AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df


def pull_rcfd_series_1(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """Pull domestic+foreign balance sheet series 1 from WRDS Call Reports.

    Fetches RMBS by maturity (rcfda555-560), first-lien residential mortgages
    by maturity (rcfda564-569), other loans by maturity (rcfda570-575), and
    cash (rcfd0010) from wrds_call_rcfd_1.

    Parameters
    ----------
    start_date : str or datetime
    end_date : str or datetime
    wrds_username : str

    Returns
    -------
    pd.DataFrame
    """
    # Note: rcfda564-569 (first-lien loans) and rcfda570-575 (other loans) do
    # not exist in wrds_call_rcfd_1. Loan maturity data is RCON-only in WRDS.
    sql = f"""
        SELECT
            b.rssd9001, b.rssd9017, b.rssd9999,
            b.rcfda555, b.rcfda556, b.rcfda557, b.rcfda558, b.rcfda559, b.rcfda560,
            b.rcfd0010
        FROM bank.wrds_call_rcfd_1 AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df


def pull_rcfd_series_2(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """Pull domestic+foreign balance sheet series 2 from WRDS Call Reports.

    Fetches treasury/other securities by maturity (rcfda549-554) and total
    assets (rcfd2170) from wrds_call_rcfd_2.

    Parameters
    ----------
    start_date : str or datetime
    end_date : str or datetime
    wrds_username : str

    Returns
    -------
    pd.DataFrame
    """
    sql = f"""
        SELECT
            b.rssd9001, b.rssd9017, b.rssd9999,
            b.rcfda549, b.rcfda550, b.rcfda551, b.rcfda552, b.rcfda553, b.rcfda554,
            b.rcfd2170
        FROM bank.wrds_call_rcfd_2 AS b
        WHERE b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df


# ---------------------------------------------------------------------------
# Load functions — read from cached parquet files
# ---------------------------------------------------------------------------


def load_rcon_series_1(data_dir=DATA_DIR):
    """Load cached RCON series 1 from _data/RCON_Series_1.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_1.parquet")


def load_rcon_series_2(data_dir=DATA_DIR):
    """Load cached RCON series 2 from _data/RCON_Series_2.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_2.parquet")


def load_rcfd_series_1(data_dir=DATA_DIR):
    """Load cached RCFD series 1 from _data/RCFD_Series_1.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_1.parquet")


def load_rcfd_series_2(data_dir=DATA_DIR):
    """Load cached RCFD series 2 from _data/RCFD_Series_2.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_2.parquet")

# ---------------------------------------------------------------------------
# Helper: deduplicate and keep the recent record feeling
# ---------------------------------------------------------------------------

def _dedupe_bank_quarter(df: pd.DataFrame, name: str = "") -> pd.DataFrame:
    """Deduplicate WRDS Call Report rows to one record per bank-quarter.

    WRDS can contain multiple filings/amendments for the same (rssd9001, rssd9999).
    We keep the last row within each bank-quarter, which is a reasonable proxy
    for keeping the most recent amended filing.
    """
    key = ["rssd9001", "rssd9999"]

    exact_dups = int(df.duplicated().sum())
    key_dups = int(df.duplicated(subset=key).sum())

    if exact_dups:
        print(f"{name}: dropping {exact_dups} exact duplicate rows")
        df = df.drop_duplicates()

    if key_dups:
        print(f"{name}: dropping {key_dups} duplicate bank-quarter rows (keeping last)")
        df = df.sort_values(key).drop_duplicates(subset=key, keep="last")

    return df



if __name__ == "__main__":
    for name, pull_fn in [
        ("RCON_Series_1", pull_rcon_series_1),
        ("RCON_Series_2", pull_rcon_series_2),
        ("RCFD_Series_1",pull_rcfd_series_1),
        ("RCFD_Series_2",pull_rcfd_series_2)
    ]:
        df = pull_fn(wrds_username=WRDS_USERNAME)
        before = len(df)
        df = _dedupe_bank_quarter(df, name=name)
        after= len(df)
        duplicates = before - after

        if duplicates:
            print(f"{name} deduplication: dropped {duplicates} rows")
        df.to_parquet(DATA_DIR / f"{name}.parquet")
        print(f"{name}: {len(df):,} rows saved")
