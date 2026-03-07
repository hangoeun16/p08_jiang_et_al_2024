"""Transform raw WRDS Call Report data into tidy per-bank asset DataFrames.

Provides functions that extract and label specific balance sheet items from
the four WRDS Call Report series (RCON_1, RCON_2, RCFD_1, RCFD_2) for a
given report date. Each function filters to a single quarter-end snapshot and
renames WRDS variable codes to human-readable maturity bucket labels:
  '<3m', '3m-1y', '1y-3y', '3y-5y', '5y-15y', '>15y'

Also provides helpers to clean ETF price data for the MTM calculation window.

Usage
-----
Import and call with loaded WRDS series DataFrames:
    from clean_data import get_rmbs, get_total_assets
    rmbs = get_rmbs(rcfd_series_1, rcon_series_1)
"""

import pandas as pd
from pathlib import Path

from settings import config

REPORT_DATE = config("REPORT_DATE")

# Maturity bucket labels in order (matches WRDS variable suffix order a-f)
BUCKET_COLS = ["<3m", "3m-1y", "1y-3y", "3y-5y", "5y-15y", ">15y"]

# Bucket renaming: WRDS codes → label (applied in each get_* function)
_RMBS_RCFD_MAP = {
    "rcfda555": "<3m", "rcfda556": "3m-1y", "rcfda557": "1y-3y",
    "rcfda558": "3y-5y", "rcfda559": "5y-15y", "rcfda560": ">15y",
}
# RMBS RCON columns are split across two WRDS tables:
# rcon_1 has rcona555 (<3m) and rcona557 (1y-3y)
# rcon_2 has rcona556 (3m-1y) and rcona558-560 (3y-5y, 5y-15y, >15y)
_RMBS_RCON1_MAP = {
    "rcona555": "<3m", "rcona557": "1y-3y",
}
_RMBS_RCON2_MAP = {
    "rcona556": "3m-1y", "rcona558": "3y-5y", "rcona559": "5y-15y", "rcona560": ">15y",
}
_TREASURY_RCFD_MAP = {
    "rcfda549": "<3m", "rcfda550": "3m-1y", "rcfda551": "1y-3y",
    "rcfda552": "3y-5y", "rcfda553": "5y-15y", "rcfda554": ">15y",
}
_TREASURY_RCON_MAP = {
    "rcona549": "<3m", "rcona550": "3m-1y", "rcona551": "1y-3y",
    "rcona552": "3y-5y", "rcona553": "5y-15y", "rcona554": ">15y",
}
_LOANS_RCON_MAP = {
    "rcona564": "<3m", "rcona565": "3m-1y", "rcona566": "1y-3y",
    "rcona567": "3y-5y", "rcona568": "5y-15y", "rcona569": ">15y",
}
_LOANS_RCFD_MAP = {
    "rcfda564": "<3m", "rcfda565": "3m-1y", "rcfda566": "1y-3y",
    "rcfda567": "3y-5y", "rcfda568": "5y-15y", "rcfda569": ">15y",
}
_OTHER_LOANS_RCFD_MAP = {
    "rcfda570": "<3m", "rcfda571": "3m-1y", "rcfda572": "1y-3y",
    "rcfda573": "3y-5y", "rcfda574": "5y-15y", "rcfda575": ">15y",
}
_OTHER_LOANS_RCON_MAP = {
    "rcona570": "<3m", "rcona571": "3m-1y", "rcona572": "1y-3y",
    "rcona573": "3y-5y", "rcona574": "5y-15y", "rcona575": ">15y",
}

_ID_RENAME = {"rssd9001": "bank_id", "rssd9017": "bank_name", "rssd9999": "report_date"}


def _filter_date(df, report_date):
    """Keep rows where rssd9999 equals report_date (supports str or Timestamp)."""
    return df[df["rssd9999"] == pd.Timestamp(report_date)]


def _extract_and_rename(df, id_map, value_map, report_date, dropna=True):
    """Select columns, filter to report_date, rename, and optionally drop NaN rows.

    Filtering happens before rename so the original WRDS column name rssd9999 is used.
    """
    cols = list(id_map.keys()) + list(value_map.keys())
    # Only select columns that exist in df
    cols = [c for c in cols if c in df.columns]
    sub = df[cols]
    # Filter to report date BEFORE renaming
    if "rssd9999" in sub.columns:
        sub = sub[sub["rssd9999"] == pd.Timestamp(report_date)]
    sub = sub.rename(columns={**id_map, **value_map})
    if dropna:
        # Only dropna on value columns that actually ended up in the DataFrame
        existing = [c for c in value_map.values() if c in sub.columns]
        if existing:
            sub = sub.dropna(subset=existing, how="all")
    return sub


def get_rmbs(rcfd_series_1, rcon_series_1, rcon_series_2, report_date=REPORT_DATE):
    """Extract RMBS (residential MBS) holdings by maturity bucket.

    Concatenates domestic+foreign (RCFD) and domestic-only (RCON) rows.
    Banks with foreign offices appear in both; domestic-only banks appear
    only in RCON. RCON RMBS columns are split across two WRDS tables:
    rcon_1 has <3m and 1y-3y; rcon_2 has 3m-1y, 3y-5y, 5y-15y, >15y.
    Both are merged by bank_id to produce the full 6-bucket RCON RMBS table.

    Parameters
    ----------
    rcfd_series_1 : pd.DataFrame
        RCFD series 1 from pull_wrds.load_rcfd_series_1().
    rcon_series_1 : pd.DataFrame
        RCON series 1 from pull_wrds.load_rcon_series_1().
    rcon_series_2 : pd.DataFrame
        RCON series 2 from pull_wrds.load_rcon_series_2().
    report_date : str
        Quarter-end snapshot date, e.g. '2022-03-31'.

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, report_date, <3m, 3m-1y, 1y-3y, 3y-5y, 5y-15y, >15y
    """
    df_rcfd = _extract_and_rename(rcfd_series_1, _ID_RENAME, _RMBS_RCFD_MAP, report_date)

    # RCON RMBS is split: merge the two partial sets on bank_id
    df_rcon1 = _extract_and_rename(rcon_series_1, _ID_RENAME, _RMBS_RCON1_MAP, report_date, dropna=False)
    df_rcon2_partial = _extract_and_rename(rcon_series_2, {"rssd9001": "bank_id", "rssd9999": "report_date"},
                                           _RMBS_RCON2_MAP, report_date, dropna=False)
    df_rcon = df_rcon1.merge(
        df_rcon2_partial[["bank_id"] + list(_RMBS_RCON2_MAP.values())],
        on="bank_id", how="outer",
    )
    df_rcon = df_rcon.dropna(subset=BUCKET_COLS, how="all")

    return pd.concat([df_rcon, df_rcfd]).sort_index().reset_index(drop=True)


def get_treasuries(rcfd_series_2, rcon_series_2, report_date=REPORT_DATE):
    """Extract treasury and non-RMBS securities holdings by maturity bucket.

    Parameters
    ----------
    rcfd_series_2 : pd.DataFrame
    rcon_series_2 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, report_date, <3m, 3m-1y, 1y-3y, 3y-5y, 5y-15y, >15y
    """
    df_rcfd = _extract_and_rename(rcfd_series_2, _ID_RENAME, _TREASURY_RCFD_MAP, report_date)
    df_rcon = _extract_and_rename(rcon_series_2, _ID_RENAME, _TREASURY_RCON_MAP, report_date)
    return pd.concat([df_rcon, df_rcfd]).sort_index().reset_index(drop=True)


def get_loans(rcon_series_1, report_date=REPORT_DATE):
    """Extract first-lien residential mortgage loans by maturity bucket (domestic).

    Parameters
    ----------
    rcon_series_1 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, report_date, <3m, 3m-1y, 1y-3y, 3y-5y, 5y-15y, >15y
    """
    df = _extract_and_rename(rcon_series_1, _ID_RENAME, _LOANS_RCON_MAP, report_date, dropna=False)
    return df.reset_index(drop=True)


def get_other_loans(rcon_series_2, rcfd_series_1, report_date=REPORT_DATE):
    """Extract non-first-lien loan holdings by maturity bucket.

    Parameters
    ----------
    rcon_series_2 : pd.DataFrame
    rcfd_series_1 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, report_date, <3m, 3m-1y, 1y-3y, 3y-5y, 5y-15y, >15y
    """
    df_rcfd = _extract_and_rename(rcfd_series_1, _ID_RENAME, _OTHER_LOANS_RCFD_MAP, report_date)
    df_rcon = _extract_and_rename(rcon_series_2, _ID_RENAME, _OTHER_LOANS_RCON_MAP, report_date)
    return pd.concat([df_rcon, df_rcfd]).sort_index().reset_index(drop=True)


def get_total_assets(rcfd_series_2, rcon_series_2, report_date=REPORT_DATE):
    """Extract total assets (consolidated) for each bank.

    Prefers rcfd2170 (domestic+foreign) when available; falls back to
    rcon2170 (domestic only) for domestic-only banks. Deduplicates by
    keeping the max value per bank ID (RCFD >= RCON always).

    Parameters
    ----------
    rcfd_series_2 : pd.DataFrame
    rcon_series_2 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, total_assets
    """
    df_rcfd = rcfd_series_2[["rssd9001", "rssd9017", "rssd9999", "rcfd2170"]].dropna()
    df_rcfd = _filter_date(df_rcfd, report_date).rename(
        columns={"rssd9001": "bank_id", "rssd9017": "bank_name",
                 "rssd9999": "report_date", "rcfd2170": "total_assets"}
    )

    df_rcon = rcon_series_2[["rssd9001", "rssd9017", "rssd9999", "rcon2170"]].dropna()
    df_rcon = _filter_date(df_rcon, report_date).rename(
        columns={"rssd9001": "bank_id", "rssd9017": "bank_name",
                 "rssd9999": "report_date", "rcon2170": "total_assets"}
    )

    combined = pd.concat([df_rcfd, df_rcon])
    # Deduplicate: for each bank keep the row with largest total_assets
    combined = (
        combined
        .sort_values("total_assets", ascending=False)
        .drop_duplicates(subset=["bank_id"])
        [["bank_id", "bank_name", "total_assets"]]
        .reset_index(drop=True)
    )
    return combined


def get_uninsured_deposits(rcon_series_1, report_date=REPORT_DATE):
    """Extract uninsured deposits (rcon5597) for each bank.

    Parameters
    ----------
    rcon_series_1 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, uninsured_deposits
    """
    df = rcon_series_1[["rssd9001", "rssd9017", "rssd9999", "rcon5597"]].copy()
    df = _filter_date(df, report_date).rename(columns={
        "rssd9001": "bank_id", "rssd9017": "bank_name",
        "rssd9999": "report_date", "rcon5597": "uninsured_deposits",
    })
    return df[["bank_id", "bank_name", "uninsured_deposits"]].reset_index(drop=True)


def get_insured_deposits(rcon_series_1, report_date=REPORT_DATE):
    """Extract insured deposits (rconf049 + rconf045) for each bank.

    rconf049: deposit accounts (excl. retirement) of $250k or less
    rconf045: retirement deposit accounts of $250k or less

    Parameters
    ----------
    rcon_series_1 : pd.DataFrame
    report_date : str

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name, insured_deposits
    """
    df = rcon_series_1[["rssd9001", "rssd9017", "rssd9999", "rconf049", "rconf045"]].copy()
    df = _filter_date(df, report_date).rename(columns={
        "rssd9001": "bank_id", "rssd9017": "bank_name", "rssd9999": "report_date",
    })
    df["insured_deposits"] = df["rconf049"].fillna(0) + df["rconf045"].fillna(0)
    return df[["bank_id", "bank_name", "insured_deposits"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# ETF data cleaning helpers
# ---------------------------------------------------------------------------


def clean_etf_prices(etf_data, start_date, end_date):
    """Resample ETF prices to quarterly frequency and filter to date range.

    Parameters
    ----------
    etf_data : pd.DataFrame
        Daily ETF prices indexed by date from pull_etf_data.load_etf_data().
    start_date : str
        Start date (inclusive), e.g. '2022-03-31'.
    end_date : str
        End date (inclusive), e.g. '2023-03-31'.

    Returns
    -------
    pd.DataFrame
        Quarterly ETF prices (first trading day of each quarter) within range.
    """
    quarterly = etf_data.resample("QE").first()
    return quarterly.loc[start_date:end_date]
