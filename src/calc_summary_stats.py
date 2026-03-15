"""Compute summary balance sheet statistics for Table A1 and Figure A1.

Aggregates individual bank holdings across all four asset categories to
compute industry-level balance sheet composition as of the report date.
This replicates Table A1 (balance sheet composition) and provides the
data for Figure A1 (aggregate assets and liabilities bar chart) from
Jiang et al. (2024).

Usage
-----
    from calc_summary_stats import calc_balance_sheet, calc_figure_a1_data,
    calc_table_a1, calc_table_a1_panel_b
"""

import numpy as np
import pandas as pd
from scipy.stats.mstats import winsorize

from clean_data import BUCKET_COLS

# ---------------------------------------------------------------------------
# Size category groups (used for table column ordering)
# ---------------------------------------------------------------------------
 
_SIZE_GROUPS = [
    ("Full sample", None),
    ("small", "Small"),
    ("large", "Large non-GSIB"),
    ("GSIB", "GSIB"),
]
 
# Standard output column order for Table A1
_TABLE_COLUMNS = [
    "Aggregate",
    "Full sample(mean)", "Full sample(sd)",
    "small(mean)", "small(sd)",
    "large(mean)", "large(sd)",
    "GSIB(mean)", "GSIB(sd)",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
 
def _total_holdings(df):
    """Sum all maturity bucket columns to get total holdings per bank.
 
    Parameters
    ----------
    df : pd.DataFrame
        Holdings data with one or more BUCKET_COLS columns.
 
    Returns
    -------
    pd.Series
        Row-wise sum across available bucket columns.
    """
    available = [b for b in BUCKET_COLS if b in df.columns]
    return df[available].fillna(0).sum(axis=1)


def _winsorized_mean_sd(x: pd.Series) -> tuple[float, float]:
    """Compute mean and standard deviation after winsorizing at 5%/95%.
 
    Parameters
    ----------
    x : pd.Series
        Numeric series (NaN values are dropped before winsorization).
 
    Returns
    -------
    tuple[float, float]
        (winsorized mean, winsorized standard deviation).
    """
    x = pd.to_numeric(x, errors="coerce").dropna().to_numpy(dtype=np.float64)
 
    if len(x) == 0:
        return np.nan, np.nan
 
    w = np.asarray(winsorize(x, limits=[0.05, 0.05]), dtype=np.float64)
 
    mean = w.mean()
    sd = w.std(ddof=1) if len(w) > 1 else np.nan
 
    return mean, sd

def _sum_by_size_category(
    holdings_df: pd.DataFrame,
    total_assets_df: pd.DataFrame,
    value_col: str = "holdings",
) -> pd.Series:
    """Sum a value column by bank size category and add a Total row.
 
    Merges holdings with size_category from total_assets_df, groups by
    size_category, and converts from $thousands to $billions.
 
    Parameters
    ----------
    holdings_df : pd.DataFrame
        Must have 'bank_id' and the column named by value_col.
    total_assets_df : pd.DataFrame
        Must have 'bank_id' and 'size_category'.
    value_col : str
        Column to sum.
 
    Returns
    -------
    pd.Series
        Indexed by size category plus 'Total', values in $billions.
    """
    merged = holdings_df.merge(
        total_assets_df[["bank_id", "size_category"]], on="bank_id", how="left"
    )
    result = merged.groupby("size_category")[value_col].sum()
    result["Total"] = merged[value_col].sum()
    return result / 1e6

# ---------------------------------------------------------------------------
# Table A1 builders
# ---------------------------------------------------------------------------

def _build_ratio_table(
    df: pd.DataFrame,
    row_items: list[str],
    include_total_asset_row: bool = True,
    include_bank_count: bool = True,
) -> pd.DataFrame:
    """Shared logic for Table A1 Panel A and Panel B.
 
    Computes aggregate ratios (item sum / total assets sum × 100) and
    winsorized per-bank ratio means and standard deviations across size
    categories.
 
    Parameters
    ----------
    df : pd.DataFrame
        Bank-level data with 'Total Asset', 'size_category', and all
        columns listed in row_items.
    row_items : list[str]
        Balance sheet items to include as rows.
    include_total_asset_row : bool
        If True, include a 'Total Asset $' row with level values.
    include_bank_count : bool
        If True, include a 'Number of Banks' row.
 
    Returns
    -------
    pd.DataFrame
        Rows = balance sheet items, columns = Aggregate + mean/sd per
        size group.
    """
    raw = df.copy()
    for c in row_items + ["Total Asset"]:
        if c in raw.columns:
            raw[c] = pd.to_numeric(raw[c], errors="coerce")
 
    raw = raw[raw["Total Asset"] > 0].copy()
 
    # Build output index
    header_rows = []
    if include_total_asset_row:
        header_rows.append("Total Asset $")
    if include_bank_count:
        header_rows.append("Number of Banks")
 
    out = pd.DataFrame(
        index=header_rows + row_items,
        columns=_TABLE_COLUMNS,
        dtype="float64",
    )
 
    total_assets_sum = raw["Total Asset"].sum()
 
    # Aggregate column: item sum / total assets × 100
    for r in row_items:
        out.loc[r, "Aggregate"] = (
            raw[r].sum() / total_assets_sum * 100 if total_assets_sum > 0 else np.nan
        )
 
    if include_total_asset_row:
        out.loc["Total Asset $", "Aggregate"] = total_assets_sum
    if include_bank_count:
        out.loc["Number of Banks", "Aggregate"] = len(raw)
 
    # Per-bank ratios: item / Total Asset × 100
    ratios = raw.copy()
    denom = ratios["Total Asset"].replace(0, np.nan)
    for r in row_items:
        ratios[r] = 100 * ratios[r] / denom
 
    # Fill mean/sd for each size group
    for label, category in _SIZE_GROUPS:
        sub = ratios if category is None else ratios[ratios["size_category"] == category]
        mean_col = f"{label}(mean)"
        sd_col = f"{label}(sd)"
 
        for r in row_items:
            m, s = _winsorized_mean_sd(sub[r])
            out.loc[r, mean_col] = m
            out.loc[r, sd_col] = s
 
        if include_total_asset_row:
            m, s = _winsorized_mean_sd(sub["Total Asset"])
            out.loc["Total Asset $", mean_col] = m
            out.loc["Total Asset $", sd_col] = s
 
        if include_bank_count:
            out.loc["Number of Banks", mean_col] = len(sub)
            out.loc["Number of Banks", sd_col] = np.nan
 
    return out.round(1)
 
 

def calc_table_a1(bank_asset: pd.DataFrame) -> pd.DataFrame:
    """Compute Table A1 Panel A (asset composition) per Jiang et al. (2024).
 
    Produces aggregate ratios and winsorized cross-sectional mean/sd of
    each asset category as a share of total assets, broken out by bank
    size category.
 
    Parameters
    ----------
    bank_asset : pd.DataFrame
        Per-bank asset frame from clean_data.build_table_a1_assets_from_raw().
        Must contain 'bank_id', 'size_category', 'Total Asset', and all
        asset item columns.
 
    Returns
    -------
    pd.DataFrame
        Rows = asset categories, columns = Aggregate + mean/sd per size
        group. Values are percentages of total assets (except Total Asset $
        and Number of Banks).
    """
    asset_rows = [
        "Cash", "Security", "Treasury", "RMBS", "CMBS", "ABS",
        "Other Security", "Total Loan", "Real Estate Loan",
        "Residential Mortgage", "Commercial Mortgage", "Other Real Estate Loan",
        "Agricultural Loan", "Commercial & Industrial Loan", "Consumer Loan",
        "Loan to Non-Depository", "Fed Funds Sold", "Reverse Repo",
    ]
 
    required = ["bank_id", "Total Asset", "size_category"] + asset_rows
    missing = [c for c in required if c not in bank_asset.columns]
    if missing:
        raise ValueError(f"calc_table_a1 missing required columns: {missing}")
 
    return _build_ratio_table(
        bank_asset, asset_rows,
        include_total_asset_row=True,
        include_bank_count=True,
    )
 
 
def calc_table_a1_panel_b(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Table A1 Panel B (liability/equity composition).
 
    Produces aggregate ratios and winsorized cross-sectional mean/sd of
    each liability/equity category as a share of total assets, broken out
    by bank size category.
 
    Parameters
    ----------
    df : pd.DataFrame
        Per-bank liability frame from
        clean_data.build_table_a1_liabilities_from_raw(). Must contain
        'bank_id', 'size_category', 'Total Asset', and all liability
        item columns.
 
    Returns
    -------
    pd.DataFrame
        Rows = liability/equity categories, columns = Aggregate + mean/sd
        per size group. Values are percentages of total assets.
    """
    ratio_items = [
        "Total Liability", "Domestic Deposit", "Insured Deposit",
        "Uninsured Deposit", "Uninsured Time Deposits",
        "Uninsured Long-Term Time Deposits", "Uninsured Short-Term Time Deposits",
        "Foreign Deposit", "Fed Fund Purchase", "Repo", "Other Liability",
        "Total Equity", "Common Stock", "Preferred Stock", "Retained Earning",
    ]
 
    return _build_ratio_table(
        df, ratio_items,
        include_total_asset_row=False,
        include_bank_count=False,
    )

# ---------------------------------------------------------------------------
# Balance sheet aggregation
# ---------------------------------------------------------------------------

def calc_balance_sheet(
    rmbs_df,
    loans_df,
    treasury_df,
    other_loans_df,
    total_assets_df,
    uninsured_df,
    insured_df,
):
    """Compute aggregate balance sheet composition across all banks.

    Sums each asset and liability category across all banks to produce
    the industry-level balance sheet in trillions of dollars.

    Parameters
    ----------
    rmbs_df : pd.DataFrame
        From clean_data.get_rmbs().
    loans_df : pd.DataFrame
        From clean_data.get_loans().
    treasury_df : pd.DataFrame
        From clean_data.get_treasuries().
    other_loans_df : pd.DataFrame
        From clean_data.get_other_loans().
    total_assets_df : pd.DataFrame
        From clean_data.get_total_assets() with size_category from classify_banks().
    uninsured_df : pd.DataFrame
        From clean_data.get_uninsured_deposits().
    insured_df : pd.DataFrame
        From clean_data.get_insured_deposits().

    Returns
    -------
    pd.DataFrame
        Rows = balance sheet items, columns = bank size categories + Total.
        Values in $billions.
    """
    def _sum_by_category(holdings_df, total_assets_df):
        """Sum holdings for each bank size category."""
        merged = holdings_df.merge(
            total_assets_df[["bank_id", "size_category"]], on="bank_id", how="left"
        )
        merged["holdings"] = _total_holdings(merged)
        result = merged.groupby("size_category")["holdings"].sum()
        result["Total"] = merged["holdings"].sum()
        return result / 1e6  # $thousands → $billions

    # Asset categories
    rmbs_by_cat = _sum_by_category(rmbs_df, total_assets_df)
    loans_by_cat = _sum_by_category(loans_df, total_assets_df)
    treasury_by_cat = _sum_by_category(treasury_df, total_assets_df)
    other_loans_by_cat = _sum_by_category(other_loans_df, total_assets_df)

    # Total assets by category
    assets_merged = total_assets_df.copy()
    total_by_cat = assets_merged.groupby("size_category")["total_assets"].sum() / 1e6
    total_by_cat["Total"] = assets_merged["total_assets"].sum() / 1e6

    # Deposit categories
    def _sum_deposits(dep_df, col, total_assets_df):
        merged = dep_df.merge(
            total_assets_df[["bank_id", "size_category"]], on="bank_id", how="left"
        )
        result = merged.groupby("size_category")[col].sum()
        result["Total"] = merged[col].sum()
        return result / 1e6

    uninsured_by_cat = _sum_deposits(uninsured_df, "uninsured_deposits", total_assets_df)
    insured_by_cat = _sum_deposits(insured_df, "insured_deposits", total_assets_df)

    table = pd.DataFrame({
        "RMBS": rmbs_by_cat,
        "First-Lien Mortgages": loans_by_cat,
        "Treasury/Other Securities": treasury_by_cat,
        "Other Loans": other_loans_by_cat,
        "Total Assets": total_by_cat,
        "Uninsured Deposits": uninsured_by_cat,
        "Insured Deposits": insured_by_cat,
    }).T

    # Ensure standard category order
    cat_order = ["Small", "Large non-GSIB", "GSIB", "Total"]
    cols_present = [c for c in cat_order if c in table.columns]
    return table[cols_present].round(1)


def calc_figure_a1_data(
    rmbs_df,
    loans_df,
    treasury_df,
    other_loans_df,
    total_assets_df,
    bank_losses_df,
    uninsured_df,
    insured_df,
):
    """Compute aggregate assets and liabilities for Figure A1 bar chart.

    Returns book-value and mark-to-market breakdowns for assets, and
    book-value liability breakdown (insured, uninsured, other).

    Parameters
    ----------
    rmbs_df, loans_df, treasury_df, other_loans_df : pd.DataFrame
        Asset holdings from clean_data.
    total_assets_df : pd.DataFrame
        Total assets with size_category.
    bank_losses_df : pd.DataFrame
        MTM losses from calc_mtm_losses.calc_bank_losses().
    uninsured_df : pd.DataFrame
        Uninsured deposits.
    insured_df : pd.DataFrame
        Insured deposits.

    Returns
    -------
    dict
        Keys: 'assets_book', 'assets_mtm', 'liabilities'
        Each is a pd.Series of $trillions indexed by category label.
    """
    # Book value asset breakdown (in $trillions)
    trillion = 1e9  # $thousands → $trillions

    rmbs_total = _total_holdings(rmbs_df).sum() / trillion
    loans_total = _total_holdings(loans_df).sum() / trillion
    treasury_total = _total_holdings(treasury_df).sum() / trillion
    other_loans_total = _total_holdings(other_loans_df).sum() / trillion
    total_assets_book = total_assets_df["total_assets"].sum() / trillion
    other_assets = total_assets_book - rmbs_total - loans_total - treasury_total - other_loans_total

    assets_book = pd.Series({
        "RMBS": rmbs_total,
        "First-Lien Mortgages": loans_total,
        "Treasury/Other Securities": treasury_total,
        "Other Loans": other_loans_total,
        "Other Assets": max(other_assets, 0),
    })

    # MTM adjustment
    total_loss_trillions = bank_losses_df["total_loss"].sum() / trillion  # negative
    rmbs_loss = bank_losses_df["rmbs_loss"].sum() / trillion
    loans_loss = bank_losses_df["loans_loss"].sum() / trillion
    treasury_loss = bank_losses_df["treasury_loss"].sum() / trillion
    other_loans_loss = bank_losses_df["other_loans_loss"].sum() / trillion

    assets_mtm = pd.Series({
        "RMBS": rmbs_total + rmbs_loss,
        "First-Lien Mortgages": loans_total + loans_loss,
        "Treasury/Other Securities": treasury_total + treasury_loss,
        "Other Loans": other_loans_total + other_loans_loss,
        "Other Assets": max(other_assets, 0),
    })

    # Liability breakdown (book value, in $trillions)
    insured_total = insured_df["insured_deposits"].sum() / trillion
    uninsured_total = uninsured_df["uninsured_deposits"].sum() / trillion
    total_dep = insured_total + uninsured_total
    other_liab = max(total_assets_book - total_dep, 0)

    liabilities = pd.Series({
        "Insured Deposits": insured_total,
        "Uninsured Deposits": uninsured_total,
        "Other Liabilities & Equity": other_liab,
    })

    return {
        "assets_book": assets_book,
        "assets_mtm": assets_mtm,
        "liabilities": liabilities,
    }
