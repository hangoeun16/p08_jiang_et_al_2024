"""Transform raw WRDS Call Report data into tidy per-bank asset DataFrames.

Provides functions that extract and label specific balance sheet items from
the four WRDS Call Report series (RCON_1, RCON_2, RCFD_1, RCFD_2) for a
given report date. Each function filters to a single quarter-end snapshot and
renames WRDS variable codes to human-readable maturity bucket labels:
  '<3m', '3m-1y', '1y-3y', '3y-5y', '5y-15y', '>15y'

Also provides helpers to clean ETF price data for the MTM calculation window
and to build per-bank asset/liability frames for Table A1 reproduction.

Usage
-----
Import and call with loaded WRDS series DataFrames:
    from clean_data import get_rmbs, get_total_assets
    rmbs = get_rmbs(rcfd_series_1, rcon_series_1)
"""

import pandas as pd
from pathlib import Path
import numpy as np


from settings import config

REPORT_DATE = config("REPORT_DATE")

# Maturity bucket labels in order (matches WRDS variable suffix order a-f)
BUCKET_COLS = ["<3m", "3m-1y", "1y-3y", "3y-5y", "5y-15y", ">15y"]

# ---------------------------------------------------------------------------
# Bucket renaming maps: WRDS codes → maturity labels
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# WRDS variable code lists for Table A1 construction
# ---------------------------------------------------------------------------

# Global/consolidated (RCFD) asset codes
_GLOBAL_RMBS = [
    "rcfdg301", "rcfdg303", "rcfdg305", "rcfdg307", "rcfdg309", "rcfdg311",
    "rcfdg313", "rcfdg315", "rcfdg317", "rcfdg319", "rcfdg321", "rcfdg323",
]
_GLOBAL_CMBS = ["rcfdk143", "rcfdk145", "rcfdk147", "rcfdk149", "rcfdk151", "rcfdk153", "rcfdk157"]
_GLOBAL_ABS = ["rcfdc988", "rcfdc027"]
_GLOBAL_OTHER_SEC = ["rcfd1738", "rcfd1741", "rcfd1743", "rcfd1746"]
_GLOBAL_RS_LOAN = [
    "rcfdf158", "rcfdf159", "rcfd1420", "rcfd1797", "rcfd5367",
    "rcfd5368", "rcfd1460", "rcfdf160", "rcfdf161",
]
_GLOBAL_RS_RESIDENTIAL = ["rcfd1420", "rcfd1797", "rcfd5367", "rcfd5368", "rcfd1460"]
_GLOBAL_RS_COMMERCIAL = ["rcfdf160", "rcfdf161"]
_GLOBAL_RS_OTHER = ["rcfdf158", "rcfdf159"]
_GLOBAL_CI_LOAN = ["rcfd1763", "rcfd1764"]
_GLOBAL_CONSUMER_LOAN = ["rcfdb538", "rcfdb539", "rcfdk137", "rcfdk207"]
 # Domestic (RCON) asset codes
_DOMESTIC_CASH = ["rcon0081", "rcon0071"]
_DOMESTIC_SEC_TOTAL = ["rcon1771", "rcon1773"]
_DOMESTIC_TREASURY = ["rcon0213", "rcon1287"]
_DOMESTIC_RMBS = [
    "rconht55", "rconht57", "rcong309", "rcong311", "rcong313",
    "rcong315", "rcong317", "rcong319", "rcong321", "rcong323",
]
_DOMESTIC_CMBS = ["rconk143", "rconk145", "rconk147", "rconk149", "rconk151", "rconk153", "rconk157"]
_DOMESTIC_ABS = ["rconc988", "rconc027", "rconht59", "rconht61"]
_DOMESTIC_OTHER_SEC = ["rcon1738", "rcon1741", "rcon1743", "rcon1746"]
_DOMESTIC_RS_LOAN = [
    "rconf158", "rconf159", "rcon1420", "rcon1797", "rcon5367",
    "rcon5368", "rcon1460", "rconf160", "rconf161",
]
_DOMESTIC_RS_RESIDENTIAL = ["rcon1420", "rcon1797", "rcon5367", "rcon5368", "rcon1460"]
_DOMESTIC_RS_COMMERCIAL = ["rconf160", "rconf161"]
_DOMESTIC_RS_OTHER = ["rconf158", "rconf159"]
_DOMESTIC_CI_LOAN = ["rcon1766"]
_DOMESTIC_CONSUMER_LOAN = ["rconb538", "rconb539", "rconk137", "rconk207"]
_DOMESTIC_NON_DEP_LOAN = ["rconj454", "rconj464", "rconj451"]
 
# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _filter_date(df, report_date):
    """Keep rows where rssd9999 equals report_date.
 
    Parameters
    ----------
    df : pd.DataFrame
        Raw WRDS series with an 'rssd9999' datetime column.
    report_date : str
        Quarter-end date string, e.g. '2022-03-31'.
 
    Returns
    -------
    pd.DataFrame
        Rows matching the requested report date.
    """
    return df[df["rssd9999"] == pd.Timestamp(report_date)]


def _extract_and_rename(df, id_map, value_map, report_date, dropna=True):
    """Select columns, filter to report_date, rename, and optionally drop NaN rows.
 
    Filtering happens before rename so the original WRDS column name rssd9999
    is used.
 
    Parameters
    ----------
    df : pd.DataFrame
        Raw WRDS series DataFrame.
    id_map : dict
        Column rename map for identifier columns (e.g. rssd9001 → bank_id).
    value_map : dict
        Column rename map for value columns (WRDS codes → bucket labels).
    report_date : str
        Quarter-end date string for filtering.
    dropna : bool
        If True, drop rows where all value columns are NaN.
 
    Returns
    -------
    pd.DataFrame
        Filtered, renamed subset.
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

def _safe_sum(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Sum multiple numeric columns, coercing errors and filling NaN with 0.
 
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the columns to sum.
    cols : list[str]
        Column names to sum. Missing columns are silently skipped.
 
    Returns
    -------
    pd.Series
        Row-wise sum across available columns.
    """
    existing = [c for c in cols if c in df.columns]
    if not existing:
        return pd.Series(0.0, index=df.index, dtype="float64")
    return df[existing].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)

def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Extract a single column as numeric, returning NaN if column is missing.
 
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to extract from.
    col : str
        Column name.
 
    Returns
    -------
    pd.Series
        Numeric series, or NaN-filled series if column does not exist.
    """
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")
 
def _first_nonnull(a: pd.Series, b: pd.Series) -> pd.Series:
    """Return the first non-null value from two series element-wise.
 
    Used to prefer RCFD (global) values and fall back to RCON (domestic).
 
    Parameters
    ----------
    a : pd.Series
        Primary series (typically RCFD).
    b : pd.Series
        Fallback series (typically RCON).
 
    Returns
    -------
    pd.Series
        Combined series using a where available, b otherwise.
    """
    a = pd.to_numeric(a, errors="coerce").astype("float64")
    b = pd.to_numeric(b, errors="coerce").astype("float64")
    return a.combine_first(b)
 
def _collapse_buckets(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """Collapse a cleaned holdings DataFrame to one row per bank_id.
 
    Sums all maturity bucket columns into a single value column and
    aggregates by bank_id.
 
    Parameters
    ----------
    df : pd.DataFrame
        Cleaned holdings with 'bank_id' and maturity bucket columns.
    value_name : str
        Name for the summed output column.
 
    Returns
    -------
    pd.DataFrame
        Columns: bank_id, bank_name (if present), value_name.
    """
    available = [c for c in BUCKET_COLS if c in df.columns]
 
    out = df[["bank_id"]].copy()
    if "bank_name" in df.columns:
        out["bank_name"] = df["bank_name"]
 
    out[value_name] = df[available].fillna(0).sum(axis=1) if available else 0
 
    agg_dict = {value_name: "sum"}
    if "bank_name" in out.columns:
        agg_dict["bank_name"] = "first"
 
    return out.groupby("bank_id", as_index=False).agg(agg_dict)
 
 
# ---------------------------------------------------------------------------
# Core extraction functions (maturity-bucketed holdings)
# ---------------------------------------------------------------------------
 
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

# ---------------------------------------------------------------------------
# Deposit extraction
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Table A1 builders
# ---------------------------------------------------------------------------

def build_table_a1_assets(
    rmbs_df: pd.DataFrame,
    treasury_df: pd.DataFrame,
    loans_df: pd.DataFrame,
    other_loans_df: pd.DataFrame,
    total_assets_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a per-bank wide asset frame for Table A1.

    Uses already-cleaned inputs and preserves the existing size_category
    classification from total_assets_df.

    Parameters
    ----------
    rmbs_df : pd.DataFrame
        RMBS holdings from get_rmbs().
    treasury_df : pd.DataFrame
        Treasury holdings from get_treasuries().
    loans_df : pd.DataFrame
        First-lien mortgage holdings from get_loans().
    other_loans_df : pd.DataFrame
        Other loan holdings from get_other_loans().
    total_assets_df : pd.DataFrame
        Must contain bank_id, bank_name, size_category, total_assets.

    Returns
    -------
    pd.DataFrame
        Columns:
        bank_id, bank_name, size_category, Total Asset,
        Security, Treasury, RMBS, Total Loan,
        Residential Mortgage, Other Loans
    """
    rmbs = _collapse_buckets(rmbs_df, "RMBS")
    treasury = _collapse_buckets(treasury_df, "Treasury")
    loans = _collapse_buckets(loans_df, "Residential Mortgage")
    other_loans = _collapse_buckets(other_loans_df, "Other Loans")
 
    assets = total_assets_df.rename(columns={"total_assets": "Total Asset"})[
        ["bank_id", "bank_name", "size_category", "Total Asset"]
    ].copy()
 
    bank_asset = (
        assets.merge(rmbs[["bank_id", "RMBS"]], on="bank_id", how="left")
              .merge(treasury[["bank_id", "Treasury"]], on="bank_id", how="left")
              .merge(loans[["bank_id", "Residential Mortgage"]], on="bank_id", how="left")
              .merge(other_loans[["bank_id", "Other Loans"]], on="bank_id", how="left")
    )
 
    for col in ["RMBS", "Treasury", "Residential Mortgage", "Other Loans"]:
        bank_asset[col] = bank_asset[col].fillna(0)
 
    # With current cleaned inputs, "Treasury" bucket is the available security measure
    bank_asset["Security"] = bank_asset["Treasury"]
    bank_asset["Total Loan"] = (
        bank_asset["Residential Mortgage"] + bank_asset["Other Loans"]
    )
 
    bank_asset = bank_asset[bank_asset["Total Asset"] > 0].copy()
 
    return bank_asset[
        [
            "bank_id", "bank_name", "size_category", "Total Asset",
            "Security", "Treasury", "RMBS", "Total Loan",
            "Residential Mortgage", "Other Loans",
        ]
    ].reset_index(drop=True)


def build_table_a1_raw_frames(
    rcon_series_1: pd.DataFrame,
    rcon_series_2: pd.DataFrame,
    rcfd_series_1: pd.DataFrame,
    rcfd_series_2: pd.DataFrame,
    rcfn_series_1: pd.DataFrame,
    report_date=REPORT_DATE,
):
    """Build raw quarter-end frames used by Table A1 construction.
 
    Filters each series to the report date, merges series 1 and 2 within
    each prefix (RCON, RCFD, RCFN), and sets rssd9001 as the index.
 
    Parameters
    ----------
    rcon_series_1 : pd.DataFrame
        RCON series 1 from pull_wrds.load_rcon_series_1().
    rcon_series_2 : pd.DataFrame
        RCON series 2 from pull_wrds.load_rcon_series_2().
    rcfd_series_1 : pd.DataFrame
        RCFD series 1 from pull_wrds.load_rcfd_series_1().
    rcfd_series_2 : pd.DataFrame
        RCFD series 2 from pull_wrds.load_rcfd_series_2().
    rcfn_series_1 : pd.DataFrame or None
        Optional RCFN series for foreign deposit data.
    report_date : str
        Quarter-end date string, e.g. '2022-03-31'.
 
    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]
    """    

    rcon_1_df = _filter_date(rcon_series_1, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcon_2_df = _filter_date(rcon_series_2, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcon_df = pd.merge(rcon_1_df, rcon_2_df, left_index=True, right_index=True, how="inner")

    rcfd_1_df = _filter_date(rcfd_series_1, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcfd_2_df = _filter_date(rcfd_series_2, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcfd_df = pd.merge(rcfd_1_df, rcfd_2_df, left_index=True, right_index=True, how="inner")

    rcfn_q_df = _filter_date(rcfn_series_1, report_date).sort_values("rssd9001").set_index("rssd9001")

    return rcon_df, rcfd_df, rcfn_q_df

def _build_asset_panel(
    df: pd.DataFrame,
    prefix: str,
) -> pd.DataFrame:
    """Build one side (RCFD or RCON) of the Table A1 asset panel.
 
    Extracts and sums the relevant WRDS variable codes for each asset
    category, using the appropriate global or domestic code lists.
 
    Parameters
    ----------
    df : pd.DataFrame
        Quarter-end merged raw frame indexed by rssd9001 (either RCFD or RCON).
    prefix : str
        Either 'rcfd' for global/consolidated or 'rcon' for domestic.
 
    Returns
    -------
    pd.DataFrame
        One row per bank with columns: Total Asset, Cash, Security,
        Treasury, RMBS, CMBS, ABS, Other Security, Total Loan,
        Real Estate Loan, Residential Mortgage, Commercial Mortgage,
        Other Real Estate Loan, Agricultural Loan, Commercial & Industrial Loan,
        Consumer Loan, Loan to Non-Depository, Fed Funds Sold, Reverse Repo.
    """
    if prefix == "rcfd":
        codes = {
            "Total Asset":                ("col", "rcfd2170"),
            "Cash":                       ("col", "rcfd0010"),
            "Security":                   ("sum", ["rcfd1771", "rcfd1773"]),
            "Treasury":                   ("sum", ["rcfd0213", "rcfd1287"]),
            "RMBS":                       ("sum", _GLOBAL_RMBS),
            "CMBS":                       ("sum", _GLOBAL_CMBS),
            "ABS":                        ("sum", _GLOBAL_ABS),
            "Other Security":             ("sum", _GLOBAL_OTHER_SEC),
            "Total Loan":                 ("col", "rcfd2122"),
            "Real Estate Loan":           ("sum", _GLOBAL_RS_LOAN),
            "Residential Mortgage":       ("sum", _GLOBAL_RS_RESIDENTIAL),
            "Commercial Mortgage":        ("sum", _GLOBAL_RS_COMMERCIAL),
            "Other Real Estate Loan":     ("sum", _GLOBAL_RS_OTHER),
            "Agricultural Loan":          ("col", "rcfd1590"),
            "Commercial & Industrial Loan": ("sum", _GLOBAL_CI_LOAN),
            "Consumer Loan":              ("sum", _GLOBAL_CONSUMER_LOAN),
            "Loan to Non-Depository":     ("const", np.nan),
            "Fed Funds Sold":             ("col", "rconb987"),
            "Reverse Repo":               ("col", "rcfdb989"),
        }
    else:
        codes = {
            "Total Asset":                ("col", "rcon2170"),
            "Cash":                       ("sum", _DOMESTIC_CASH),
            "Security":                   ("sum", _DOMESTIC_SEC_TOTAL),
            "Treasury":                   ("sum", _DOMESTIC_TREASURY),
            "RMBS":                       ("sum", _DOMESTIC_RMBS),
            "CMBS":                       ("sum", _DOMESTIC_CMBS),
            "ABS":                        ("sum", _DOMESTIC_ABS),
            "Other Security":             ("sum", _DOMESTIC_OTHER_SEC),
            "Total Loan":                 ("col", "rcon2122"),
            "Real Estate Loan":           ("sum", _DOMESTIC_RS_LOAN),
            "Residential Mortgage":       ("sum", _DOMESTIC_RS_RESIDENTIAL),
            "Commercial Mortgage":        ("sum", _DOMESTIC_RS_COMMERCIAL),
            "Other Real Estate Loan":     ("sum", _DOMESTIC_RS_OTHER),
            "Agricultural Loan":          ("col", "rcon1590"),
            "Commercial & Industrial Loan": ("sum", _DOMESTIC_CI_LOAN),
            "Consumer Loan":              ("sum", _DOMESTIC_CONSUMER_LOAN),
            "Loan to Non-Depository":     ("sum", _DOMESTIC_NON_DEP_LOAN),
            "Fed Funds Sold":             ("col", "rconb987"),
            "Reverse Repo":               ("col", "rconb989"),
        }
 
    panel = pd.DataFrame(index=df.index)
    for label, (mode, ref) in codes.items():
        if mode == "col":
            panel[label] = _safe_col(df, ref)
        elif mode == "sum":
            panel[label] = _safe_sum(df, ref)
        elif mode == "const":
            panel[label] = ref
 
    return panel

def build_table_a1_assets_from_raw(
    rcon_df: pd.DataFrame,
    rcfd_df: pd.DataFrame,
    rcfn_df: pd.DataFrame,
    total_assets_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reproduce the notebook-style bank_asset frame for Table A1 Panel A.

    Parameters
    ----------
    rcon_df, rcfd_df, rcfn_df : pd.DataFrame
        Quarter-end raw frames indexed by rssd9001.
    total_assets_df : pd.DataFrame
        Must already contain bank_id, bank_name, size_category, total_assets.

    Returns
    -------
    pd.DataFrame
        One row per bank with notebook-style Panel A columns plus
        bank_id, bank_name, size_category.
    """
    rcfd_data = _build_asset_panel(rcfd_df, "rcfd")
    rcon_data = _build_asset_panel(rcon_df, "rcon")
 
    main_cols = list(rcfd_data.columns)
 
    bank_asset = pd.merge(
        rcfd_data, rcon_data,
        left_index=True, right_index=True, how="outer",
        suffixes=("", "_domestic"),
    )
 
    # Fill missing global values with domestic values for domestic-only banks
    replace_index = bank_asset[bank_asset["Cash"].isna()].index
    for base_col in main_cols:
        domestic_col = f"{base_col}_domestic"
        if domestic_col in bank_asset.columns:
            bank_asset.loc[replace_index, base_col] = bank_asset.loc[replace_index, domestic_col]
 
    # Loan to Non-Depository is only available domestically
    if "Loan to Non-Depository_domestic" in bank_asset.columns:
        bank_asset["Loan to Non-Depository"] = bank_asset["Loan to Non-Depository_domestic"]
 
    domestic_cols = [f"{c}_domestic" for c in main_cols]
    existing_domestic = [c for c in domestic_cols if c in bank_asset.columns]
    bank_asset = bank_asset.drop(columns=existing_domestic)
 
    bank_asset = bank_asset.reset_index().rename(columns={"rssd9001": "bank_id"})
    bank_asset = bank_asset.merge(
        total_assets_df[["bank_id", "bank_name", "size_category"]],
        on="bank_id",
        how="left",
    )
 
    ordered_cols = [
        "bank_id", "bank_name", "size_category",
        "Total Asset", "Cash", "Security", "Treasury", "RMBS", "CMBS", "ABS",
        "Other Security", "Total Loan", "Real Estate Loan", "Residential Mortgage",
        "Commercial Mortgage", "Other Real Estate Loan", "Agricultural Loan",
        "Commercial & Industrial Loan", "Consumer Loan", "Loan to Non-Depository",
        "Fed Funds Sold", "Reverse Repo",
    ]
    return bank_asset[ordered_cols]
 

def build_table_a1_liabilities_from_raw(
    rcon_df: pd.DataFrame,
    rcfd_df: pd.DataFrame,
    rcfn_df: pd.DataFrame,
    total_assets_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build per-bank liability/equity frame for Table A1 Panel B.
 
    Prefers global (RCFD) values where available and falls back to domestic
    (RCON) values using _first_nonnull.
 
    Parameters
    ----------
    rcon_df : pd.DataFrame
        Quarter-end domestic raw frame indexed by rssd9001.
    rcfd_df : pd.DataFrame
        Quarter-end global raw frame indexed by rssd9001.
    rcfn_df : pd.DataFrame or None
        Quarter-end RCFN raw frame for foreign deposit data.
    total_assets_df : pd.DataFrame
        Must contain bank_id, bank_name, size_category, total_assets.
 
    Returns
    -------
    pd.DataFrame
        One row per bank with columns: bank_id, bank_name, size_category,
        Total Asset, Total Liability, Domestic Deposit, Insured Deposit,
        Uninsured Deposit, Uninsured Time Deposits,
        Uninsured Long-Term Time Deposits, Uninsured Short-Term Time Deposits,
        Foreign Deposit, Fed Fund Purchase, Repo, Other Liability,
        Total Equity, Common Stock, Preferred Stock, Retained Earning.
    """
    # Use a reindexed safe_col that aligns to the asset_base index
    asset_base = total_assets_df[["bank_id", "bank_name", "size_category", "total_assets"]].copy()
    asset_base["bank_id"] = pd.to_numeric(asset_base["bank_id"], errors="coerce")
    asset_base = asset_base.rename(columns={"total_assets": "Total Asset"}).set_index("bank_id")
 
    def _reindex_col(df, col):
        """Extract a numeric column and reindex to align with asset_base."""
        if df is None or col not in df.columns:
            return pd.Series(np.nan, index=asset_base.index, dtype="float64")
        s = pd.to_numeric(df[col], errors="coerce").astype("float64")
        if "bank_id" in df.columns:
            s.index = pd.to_numeric(df["bank_id"], errors="coerce")
        return s.reindex(asset_base.index)
 
    def _reindex_first(rcfd_col, rcon_col):
        """Prefer RCFD, fall back to RCON, aligned to asset_base index."""
        return _first_nonnull(_reindex_col(rcfd_df, rcfd_col), _reindex_col(rcon_df, rcon_col))
 
    bank_liab = asset_base.copy()
 
    bank_liab["Total Liability"] = _reindex_first("rcfd2948", "rcon2948")
    bank_liab["Domestic Deposit"] = _reindex_col(rcon_df, "rcon2200")
 
    bank_liab["Insured Deposit"] = (
        _reindex_col(rcon_df, "rconf049").fillna(0)
        + _reindex_col(rcon_df, "rconf045").fillna(0)
    )
 
    bank_liab["Uninsured Deposit"] = (
        bank_liab["Domestic Deposit"].fillna(0)
        - bank_liab["Insured Deposit"].fillna(0)
    )
 
    bank_liab["Uninsured Time Deposits"] = _reindex_col(rcon_df, "rconj474").fillna(0)
 
    bank_liab["Uninsured Long-Term Time Deposits"] = (
        _reindex_col(rcon_df, "rconhk14").fillna(0)
        + _reindex_col(rcon_df, "rconhk15").fillna(0)
    )
 
    bank_liab["Uninsured Short-Term Time Deposits"] = _reindex_col(rcon_df, "rconk222").fillna(0)
    bank_liab["Foreign Deposit"] = _reindex_col(rcfn_df, "rcfn2200").fillna(0)
    bank_liab["Fed Fund Purchase"] = _reindex_col(rcon_df, "rconb993").fillna(0)
    bank_liab["Repo"] = _reindex_first("rcfdb995", "rconb995").fillna(0)
    bank_liab["Other Liability"] = _reindex_first("rcfd2930", "rcon2930")
    bank_liab["Total Equity"] = _reindex_first("rcfdg105", "rcong105")
    bank_liab["Common Stock"] = _reindex_first("rcfd3230", "rcon3230").fillna(0)
    bank_liab["Preferred Stock"] = _reindex_first("rcfd3838", "rcon3838").fillna(0)
    bank_liab["Retained Earning"] = _reindex_first("rcfd3632", "rcon3632").fillna(0)
 
    bank_liab = bank_liab.reset_index()
 
    ordered_cols = [
        "bank_id", "bank_name", "size_category",
        "Total Asset", "Total Liability", "Domestic Deposit", "Insured Deposit",
        "Uninsured Deposit", "Uninsured Time Deposits",
        "Uninsured Long-Term Time Deposits", "Uninsured Short-Term Time Deposits",
        "Foreign Deposit", "Fed Fund Purchase", "Repo", "Other Liability",
        "Total Equity", "Common Stock", "Preferred Stock", "Retained Earning",
    ]
    return bank_liab[ordered_cols]