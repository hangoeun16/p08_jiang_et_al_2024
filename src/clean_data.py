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
import numpy as np


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

def build_table_a1_raw_frames(
    rcon_series_1: pd.DataFrame,
    rcon_series_2: pd.DataFrame,
    rcfd_series_1: pd.DataFrame,
    rcfd_series_2: pd.DataFrame,
    rcfn_df: pd.DataFrame | None = None,
    report_date=REPORT_DATE,
):
    """
    Build raw quarter-end frames used by the notebook-style Table A1 construction.
    """

    rcon_1_df = _filter_date(rcon_series_1, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcon_2_df = _filter_date(rcon_series_2, report_date).sort_values("rssd9001").set_index("rssd9001")
    # Drop overlapping columns from series_2 (keep series_1 version)
    rcon_overlap = [c for c in rcon_2_df.columns if c in rcon_1_df.columns]
    rcon_df = pd.merge(rcon_1_df, rcon_2_df.drop(columns=rcon_overlap), left_index=True, right_index=True, how="inner")

    rcfd_1_df = _filter_date(rcfd_series_1, report_date).sort_values("rssd9001").set_index("rssd9001")
    rcfd_2_df = _filter_date(rcfd_series_2, report_date).sort_values("rssd9001").set_index("rssd9001")
    # Drop overlapping columns from series_2 (keep series_1 version)
    rcfd_overlap = [c for c in rcfd_2_df.columns if c in rcfd_1_df.columns]
    rcfd_df = pd.merge(rcfd_1_df, rcfd_2_df.drop(columns=rcfd_overlap), left_index=True, right_index=True, how="inner")

    if rcfn_df is None:
        rcfn_q_df = None
    else:
        rcfn_q_df = _filter_date(rcfn_df, report_date).sort_values("rssd9001").set_index("rssd9001")

    # Ensure numeric index for consistent merging (FFIEC uses str, WRDS uses int)
    for df in [rcon_df, rcfd_df]:
        df.index = pd.to_numeric(df.index, errors="coerce")
    if rcfn_q_df is not None:
        rcfn_q_df.index = pd.to_numeric(rcfn_q_df.index, errors="coerce")

    return rcon_df, rcfd_df, rcfn_q_df



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

    def safe_sum(df: pd.DataFrame, cols: list[str]) -> pd.Series:
        existing = [c for c in cols if c in df.columns]
        if not existing:
            return pd.Series(0.0, index=df.index, dtype="float64")
        return df[existing].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)

    def safe_col(df: pd.DataFrame, col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index, dtype="float64")
        return pd.to_numeric(df[col], errors="coerce")

    global_rmbs = [
        "rcfdg301","rcfdg303","rcfdg305","rcfdg307","rcfdg309","rcfdg311",
        "rcfdg313","rcfdg315","rcfdg317","rcfdg319","rcfdg321","rcfdg323",
    ]
    global_cmbs = ["rcfdk143","rcfdk145","rcfdk147","rcfdk149","rcfdk151","rcfdk153","rcfdk157"]
    global_abs = ["rcfdc988","rcfdc027"]
    global_other = ["rcfd1738","rcfd1741","rcfd1743","rcfd1746"]

    global_rs_loan = [
        "rcfdf158","rcfdf159","rcfd1420","rcfd1797","rcfd5367",
        "rcfd5368","rcfd1460","rcfdf160","rcfdf161",
    ]
    global_rs_residential_loan = ["rcfd1420","rcfd1797","rcfd5367","rcfd5368","rcfd1460"]
    global_rs_commercial_loan = ["rcfdf160","rcfdf161"]
    global_rs_other_loan = ["rcfdf158","rcfdf159"]
    global_ci_loan = ["rcfd1763","rcfd1764"]
    global_consumer_loan = ["rcfdb538","rcfdb539","rcfdk137","rcfdk207"]

    domestic_cash = ["rcon0081","rcon0071"]
    domestic_total = ["rcon1771","rcon1773"]
    domestic_treasury = ["rcon0213","rcon1287"]
    domestic_rmbs = [
        "rconht55","rconht57","rcong309","rcong311","rcong313",
        "rcong315","rcong317","rcong319","rcong321","rcong323",
    ]
    domestic_cmbs = ["rconk143","rconk145","rconk147","rconk149","rconk151","rconk153","rconk157"]
    domestic_abs = ["rconc988","rconc027","rconht59","rconht61"]
    domestic_other = ["rcon1738","rcon1741","rcon1743","rcon1746"]

    domestic_rs_loan = [
        "rconf158","rconf159","rcon1420","rcon1797","rcon5367",
        "rcon5368","rcon1460","rconf160","rconf161",
    ]
    domestic_rs_residential_loan = ["rcon1420","rcon1797","rcon5367","rcon5368","rcon1460"]
    domestic_rs_commercial_loan = ["rconf160","rconf161"]
    domestic_rs_other_loan = ["rconf158","rconf159"]
    domestic_ci_loan = ["rcon1766"]
    domestic_consumer_loan = ["rconb538","rconb539","rconk137","rconk207"]
    domestic_non_rep_loan = ["rconj454","rconj464","rconj451"]

    # Global/consolidated (RCFD)
    rcfd_data = pd.DataFrame(index=rcfd_df.index)
    rcfd_data["Total Asset"] = safe_col(rcfd_df, "rcfd2170")
    rcfd_data["Cash"] = safe_col(rcfd_df, "rcfd0010")
    rcfd_data["Security"] = safe_sum(rcfd_df, ["rcfd1771", "rcfd1773"])
    rcfd_data["Treasury"] = safe_sum(rcfd_df, ["rcfd0213", "rcfd1287"])
    rcfd_data["RMBS"] = safe_sum(rcfd_df, global_rmbs)
    rcfd_data["CMBS"] = safe_sum(rcfd_df, global_cmbs)
    rcfd_data["ABS"] = safe_sum(rcfd_df, global_abs)
    rcfd_data["Other Security"] = safe_sum(rcfd_df, global_other)
    rcfd_data["Total Loan"] = safe_col(rcfd_df, "rcfd2122")
    rcfd_data["Real Estate Loan"] = safe_sum(rcfd_df, global_rs_loan)
    rcfd_data["Residential Mortgage"] = safe_sum(rcfd_df, global_rs_residential_loan)
    rcfd_data["Commercial Mortgage"] = safe_sum(rcfd_df, global_rs_commercial_loan)
    rcfd_data["Other Real Estate Loan"] = safe_sum(rcfd_df, global_rs_other_loan)
    rcfd_data["Agricultural Loan"] = safe_col(rcfd_df, "rcfd1590")
    rcfd_data["Commercial & Industrial Loan"] = safe_sum(rcfd_df, global_ci_loan)
    rcfd_data["Consumer Loan"] = safe_sum(rcfd_df, global_consumer_loan)
    rcfd_data["Loan to Non-Depository"] = np.nan
    rcfd_data["Fed Funds Sold"] = safe_col(rcon_df, "rconb987")
    rcfd_data["Reverse Repo"] = safe_col(rcfd_df, "rcfdb989")

    # Domestic (RCON)
    rcon_data = pd.DataFrame(index=rcon_df.index)
    rcon_data["Total Asset"] = safe_col(rcon_df, "rcon2170")
    rcon_data["Cash"] = safe_sum(rcon_df, domestic_cash)
    rcon_data["Security"] = safe_sum(rcon_df, domestic_total)
    rcon_data["Treasury"] = safe_sum(rcon_df, domestic_treasury)
    rcon_data["RMBS"] = safe_sum(rcon_df, domestic_rmbs)
    rcon_data["CMBS"] = safe_sum(rcon_df, domestic_cmbs)
    rcon_data["ABS"] = safe_sum(rcon_df, domestic_abs)
    rcon_data["Other Security"] = safe_sum(rcon_df, domestic_other)
    rcon_data["Total Loan"] = safe_col(rcon_df, "rcon2122")
    rcon_data["Real Estate Loan"] = safe_sum(rcon_df, domestic_rs_loan)
    rcon_data["Residential Mortgage"] = safe_sum(rcon_df, domestic_rs_residential_loan)
    rcon_data["Commercial Mortgage"] = safe_sum(rcon_df, domestic_rs_commercial_loan)
    rcon_data["Other Real Estate Loan"] = safe_sum(rcon_df, domestic_rs_other_loan)
    rcon_data["Agricultural Loan"] = safe_col(rcon_df, "rcon1590")
    rcon_data["Commercial & Industrial Loan"] = safe_sum(rcon_df, domestic_ci_loan)
    rcon_data["Consumer Loan"] = safe_sum(rcon_df, domestic_consumer_loan)
    rcon_data["Loan to Non-Depository"] = safe_sum(rcon_df, domestic_non_rep_loan)
    rcon_data["Fed Funds Sold"] = safe_col(rcon_df, "rconb987")
    rcon_data["Reverse Repo"] = safe_col(rcon_df, "rconb989")


    bank_asset = pd.merge(
        rcfd_data, rcon_data,
        left_index=True, right_index=True, how="outer",
        suffixes=("", "_domestic")
    )

    main_cols = [
        "Total Asset", "Cash", "Security", "Treasury", "RMBS", "CMBS", "ABS",
        "Other Security", "Total Loan", "Real Estate Loan", "Residential Mortgage",
        "Commercial Mortgage", "Other Real Estate Loan", "Agricultural Loan",
        "Commercial & Industrial Loan", "Consumer Loan", "Loan to Non-Depository",
        "Fed Funds Sold", "Reverse Repo",
    ]

    replace_index = bank_asset[bank_asset["Cash"].isna()].index
    domestic_cols = [f"{c}_domestic" for c in main_cols]
    existing_domestic = [c for c in domestic_cols if c in bank_asset.columns]

    for base_col in main_cols:
        domestic_col = f"{base_col}_domestic"
        if domestic_col in bank_asset.columns:
            bank_asset.loc[replace_index, base_col] = bank_asset.loc[replace_index, domestic_col]


    if "Loan to Non-Depository_domestic" in bank_asset.columns:
        bank_asset["Loan to Non-Depository"] = bank_asset["Loan to Non-Depository_domestic"]

    bank_asset = bank_asset.drop(columns=existing_domestic)

    bank_asset = bank_asset.reset_index().rename(columns={"rssd9001": "bank_id"})
    bank_asset["bank_id"] = pd.to_numeric(bank_asset["bank_id"], errors="coerce")
    ta_merge = total_assets_df[["bank_id", "bank_name", "size_category"]].copy()
    ta_merge["bank_id"] = pd.to_numeric(ta_merge["bank_id"], errors="coerce")
    bank_asset = bank_asset.merge(
        ta_merge,
        on="bank_id",
        how="left",
    )

    ordered_cols = [
        "bank_id",
        "bank_name",
        "size_category",
        "Total Asset",
        "Cash",
        "Security",
        "Treasury",
        "RMBS",
        "CMBS",
        "ABS",
        "Other Security",
        "Total Loan",
        "Real Estate Loan",
        "Residential Mortgage",
        "Commercial Mortgage",
        "Other Real Estate Loan",
        "Agricultural Loan",
        "Commercial & Industrial Loan",
        "Consumer Loan",
        "Loan to Non-Depository",
        "Fed Funds Sold",
        "Reverse Repo",
    ]
    return bank_asset[ordered_cols]

def build_table_a1_liabilities_from_raw(
    rcon_df: pd.DataFrame,
    rcfd_df: pd.DataFrame,
    rcfn_df: pd.DataFrame | None,
    total_assets_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build per-bank liability/equity frame for Table A1 Panel B.

    Output columns:
      bank_id, bank_name, size_category,
      Total Asset, Total Liability, Domestic Deposit, Insured Deposit,
      Uninsured Deposit, Uninsured Time Deposits,
      Uninsured Long-Term Time Deposits, Uninsured Short-Term Time Deposits,
      Foreign Deposit, Fed Fund Purchase, Repo, Other Liability,
      Total Equity, Common Stock, Preferred Stock, Retained Earning
    """
    def safe_col(df: pd.DataFrame, col: str) -> pd.Series:
        if df is None or col not in df.columns:
            return pd.Series(np.nan, index=asset_base.index, dtype="float64")
        s = pd.to_numeric(df[col], errors="coerce").astype("float64")
        if "bank_id" in df.columns:
            s.index = pd.to_numeric(df["bank_id"], errors="coerce")
        return s.reindex(asset_base.index)

    def first_nonnull(a: pd.Series, b: pd.Series) -> pd.Series:
        a = pd.to_numeric(a, errors="coerce").astype("float64")
        b = pd.to_numeric(b, errors="coerce").astype("float64")
        return a.combine_first(b)

    asset_base = total_assets_df[["bank_id", "bank_name", "size_category", "total_assets"]].copy()
    asset_base["bank_id"] = pd.to_numeric(asset_base["bank_id"], errors="coerce")
    asset_base = asset_base.rename(columns={"total_assets": "Total Asset"}).set_index("bank_id")

    bank_liab = asset_base.copy()

    # Core liabilities
    bank_liab["Total Liability"] = first_nonnull(
        safe_col(rcfd_df, "rcfd2948"),
        safe_col(rcon_df, "rcon2948"),
    )

    bank_liab["Domestic Deposit"] = safe_col(rcon_df, "rcon2200")

    bank_liab["Insured Deposit"] = (
    safe_col(rcon_df, "rconf049").fillna(0)
    + safe_col(rcon_df, "rconf045").fillna(0)
    )

    bank_liab["Uninsured Deposit"] = (
        bank_liab["Domestic Deposit"].fillna(0)
        - bank_liab["Insured Deposit"].fillna(0)
    )

    bank_liab["Uninsured Time Deposits"] = safe_col(rcon_df, "rconj474").fillna(0)

    bank_liab["Uninsured Long-Term Time Deposits"] = (
        safe_col(rcon_df, "rconhk14").fillna(0)
        + safe_col(rcon_df, "rconhk15").fillna(0)
    )

    bank_liab["Uninsured Short-Term Time Deposits"] = safe_col(rcon_df, "rconk222").fillna(0)

    bank_liab["Foreign Deposit"] = safe_col(rcfn_df, "rcfn2200").fillna(0)

    bank_liab["Fed Fund Purchase"] = safe_col(rcon_df, "rconb993").fillna(0)

    bank_liab["Repo"] = first_nonnull(
        safe_col(rcfd_df, "rcfdb995"),
        safe_col(rcon_df, "rconb995"),
    ).fillna(0)

    bank_liab["Other Liability"] = first_nonnull(
        safe_col(rcfd_df, "rcfd2930"),
        safe_col(rcon_df, "rcon2930"),
    )

    bank_liab["Total Equity"] = first_nonnull(
        safe_col(rcfd_df, "rcfdg105"),
        safe_col(rcon_df, "rcong105"),
    )

    bank_liab["Common Stock"] = first_nonnull(
        safe_col(rcfd_df, "rcfd3230"),
        safe_col(rcon_df, "rcon3230"),
    ).fillna(0)

    bank_liab["Preferred Stock"] = first_nonnull(
        safe_col(rcfd_df, "rcfd3838"),
        safe_col(rcon_df, "rcon3838"),
    ).fillna(0)

    bank_liab["Retained Earning"] = first_nonnull(
        safe_col(rcfd_df, "rcfd3632"),
        safe_col(rcon_df, "rcon3632"),
    ).fillna(0)

    bank_liab = bank_liab.reset_index()

    ordered_cols = [
        "bank_id",
        "bank_name",
        "size_category",
        "Total Asset",
        "Total Liability",
        "Domestic Deposit",
        "Insured Deposit",
        "Uninsured Deposit",
        "Uninsured Time Deposits",
        "Uninsured Long-Term Time Deposits",
        "Uninsured Short-Term Time Deposits",
        "Foreign Deposit",
        "Fed Fund Purchase",
        "Repo",
        "Other Liability",
        "Total Equity",
        "Common Stock",
        "Preferred Stock",
        "Retained Earning",
    ]
    return bank_liab[ordered_cols]