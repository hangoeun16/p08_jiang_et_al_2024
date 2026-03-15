"""Pull and cache WRDS Call Report data needed to replicate Jiang et al. (2024).

Fetches five Call Report data series from WRDS via SQL:
  - RCON_Series_1: domestic balance sheet items (RMBS, first-lien mortgages,
    uninsured deposits, insured deposits) from wrds_call_rcon_1
  - RCON_Series_2: domestic securities (treasuries/other), other loans,
    total assets from wrds_call_rcon_2
  - RCFD_Series_1: domestic+foreign balance sheet items (RMBS, first-lien
    mortgages, other loans, cash) from wrds_call_rcfd_1
  - RCFD_Series_2: domestic+foreign treasury securities, total assets
    from wrds_call_rcfd_2
  - RCFN_Series_1_ffiec: foreign office deposits from wrds_call_rcfn_1 

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
            rcon_1.rssd9999, rcon_1.rssd9001,rcon_1.rssd9017,
            rcon_1.RCON0071, rcon_1.RCON1773, rcon_1.RCONHT55,
            rcon_1.RCONHT57, rcon_1.RCONG309, rcon_1.RCONG311,
            rcon_1.RCONG313, rcon_1.RCONG315, rcon_1.RCONG317,
            rcon_1.RCONG319, rcon_1.RCONG321, rcon_1.RCONG323,
            rcon_1.RCONK143, rcon_1.RCONK145, rcon_1.RCONK147,
            rcon_1.RCONK149, rcon_1.RCONK151, rcon_1.RCONK153,
            rcon_1.RCONK155, rcon_1.RCONK157, rcon_1.RCONC988,
            rcon_1.RCONC027, rcon_1.RCONHT59, rcon_1.RCONHT61,
            rcon_1.RCON1743, rcon_1.RCON1746, rcon_1.RCONF158,
            rcon_1.RCONF159, rcon_1.RCON5367, rcon_1.RCON5368,
            rcon_1.RCONF160, rcon_1.RCONF161, rcon_1.RCON1590,
            rcon_1.RCON1766, rcon_1.RCONB538, rcon_1.RCONK137,
            rcon_1.RCONK207, rcon_1.rconj454, 
            rcon_1.RCONB987, rcon_1.RCONJ451,
            rcon_1.rconmt91, rcon_1.rconmt87, rcon_1.rconhk14, rcon_1.rconhk15,
            rcon_1.rconb993, rcon_1.rcon3230,
            rcon_1.rcona555, rcon_1.rcona557,
            rcon_1.rcona564, rcon_1.rcona565, rcon_1.rcona566, rcon_1.rcona567, 
            rcon_1.rcona568, rcon_1.rcona569,
            rcon_1.rcon5597,
            rcon_1.rconf049, rcon_1.rconf045

        FROM bank.wrds_call_rcon_1 AS rcon_1
        WHERE rcon_1.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
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
            rcon_2.rssd9001,rcon_2.rssd9017, rcon_2.rssd9999,
            rcon_2.rcon0081, rcon_2.rcon1771, rcon_2.rcon0213, 
            rcon_2.rcon1287, rcon_2.rcon1738, rcon_2.rcon1741, rcon_2.rcon2122,
            rcon_2.rcon1420, rcon_2.rcon1797, rcon_2.rcon1460, rcon_2.rconb539,
            rcon_2.rconj464, rcon_2.rconb989,
            rcon_2.rcon2200, rcon_2.rconhk05, rcon_2.rconj474, rcon_2.rconb995,
            rcon_2.rconk222, rcon_2.rcon2948, rcon_2.rcon2930, rcon_2.rcong105,
            rcon_2.rcon3838, rcon_2.rcon3632, rcon_2.rcon2170,
            rcon_2.rcona549, rcon_2.rcona550, rcon_2.rcona551, rcon_2.rcona552,
            rcon_2.rcona553, rcon_2.rcona554, rcon_2.rcona556, rcon_2.rcona558,
            rcon_2.rcona559, rcon_2.rcona560, rcon_2.rcona570, rcon_2.rcona571,
            rcon_2.rcona572, rcon_2.rcona573, rcon_2.rcona574, rcon_2.rcona575
        FROM bank.wrds_call_rcon_2 AS rcon_2
        WHERE rcon_2.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
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
            rcfd_1.rssd9001, rcfd_1.rssd9017,rcfd_1.rssd9999, 
            rcfd_1.rcfd0010,rcfd_1.rcfd1773,rcfd_1.rcfdg301,rcfd_1.rcfdg303,
            rcfd_1.rcfdg305,rcfd_1.rcfdg307,rcfd_1.rcfdg309,rcfd_1.rcfdg311,
            rcfd_1.rcfdg313,rcfd_1.rcfdg315,rcfd_1.rcfdg317,rcfd_1.rcfdg319,
            rcfd_1.rcfdg321,rcfd_1.rcfdg323,rcfd_1.rcfdk143,rcfd_1.rcfdk145,
            rcfd_1.rcfdk147,rcfd_1.rcfdk149, rcfd_1.rcfdk151,rcfd_1.rcfdk153,
            rcfd_1.rcfdk155,rcfd_1.rcfdk157, rcfd_1.rcfdc988,rcfd_1.rcfdc027,
            rcfd_1.RCFD1738,rcfd_1.RCFD1741, rcfd_1.RCFD1743,rcfd_1.RCFD1746,
            rcfd_1.rcfdf158,rcfd_1.rcfdf159, rcfd_1.rcfd5367,rcfd_1.rcfd5368,
            rcfd_1.rcfdf160,rcfd_1.rcfdf161, rcfd_1.rcfd1590,rcfd_1.rcfd1763,
            rcfd_1.rcfd1764,rcfd_1.rcfdb538, rcfd_1.rcfdb539,
            rcfd_1.rcfdk137,rcfd_1.rcfdk207, 
            rcfd_1.rcfd2930,rcfd_1.rcfd3230,
            rcfd_1.rcfda556, rcfd_1.rcfda557,
            rcfd_1.rcfda558, rcfd_1.rcfda559, rcfd_1.rcfda560
        FROM bank.wrds_call_rcfd_1 AS rcfd_1
        WHERE rcfd_1.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
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
            rcfd_2.rssd9001, rcfd_2.rssd9017, rcfd_2.rssd9999,
            rcfd_2.rcfd1771, rcfd_2.rcfd0213, rcfd_2.rcfd1287, rcfd_2.rcfd2122,
            rcfd_2.rcfd1420, rcfd_2.rcfd1797, rcfd_2.rcfd1460, rcfd_2.rcfdb989,
            rcfd_2.rcfd2948, rcfd_2.rcfdg105, rcfd_2.rcfd3838, rcfd_2.rcfd3632,
            rcfd_2.rcfda549, rcfd_2.rcfda550, rcfd_2.rcfda551, rcfd_2.rcfda552, 
            rcfd_2.rcfda553, rcfd_2.rcfda554,
            rcfd_2.rcfd2170
        FROM bank.wrds_call_rcfd_2 AS rcfd_2
        WHERE rcfd_2.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df

def pull_rcfn_series_1(start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME):
    """
    Pull foreign-office deposit item needed for Table A1 Panel B.
    from WRDS Call Reports.

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
            rcfn_1.rssd9001,
            rcfn_1.rssd9017,
            rcfn_1.rssd9999,
            rcfn_1.rcfn2200
        FROM bank.wrds_call_rcfn_1 AS rcfn_1
        WHERE rcfn_1.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
    """
    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(sql, date_cols=["rssd9999"])
    db.close()
    return df\
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

def load_rcfn_series_1(data_dir=DATA_DIR):
    """Load cached RCFN series 1 from _data/RCFN_Series_1.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFN_Series_1.parquet")

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
        ("RCFD_Series_2",pull_rcfd_series_2),
        ("RCFN_Series_1",pull_rcfn_series_1)
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
