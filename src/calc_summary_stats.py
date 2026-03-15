"""Compute summary balance sheet statistics for Table A1 and Figure A1.

Aggregates individual bank holdings across all four asset categories to
compute industry-level balance sheet composition as of the report date.
This replicates Table A1 (balance sheet composition) and provides the
data for Figure A1 (aggregate assets and liabilities bar chart) from
Jiang et al. (2024).

Usage
-----
    from calc_summary_stats import calc_balance_sheet, calc_figure_a1_data
"""

import numpy as np
import pandas as pd

from clean_data import BUCKET_COLS


def _total_holdings(df):
    """Sum all maturity bucket columns to get total holdings per bank."""
    available = [b for b in BUCKET_COLS if b in df.columns]
    return df[available].fillna(0).sum(axis=1)

def _winsorize_series(s: pd.Series, lower: float = 0.05, upper: float = 0.95) -> pd.Series:
    """Winsorize a series at the given lower/upper quantiles."""
    s = pd.to_numeric(s, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        return s
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lower=lo, upper=hi)


def _winsorized_mean_sd(x: pd.Series) -> tuple[float, float]:
    x = pd.to_numeric(x, errors="coerce").dropna().to_numpy(dtype=np.float64)

    if len(x) == 0:
        return np.nan, np.nan

    w = winsorize(x, limits=[0.05, 0.05])
    w = np.asarray(w, dtype=np.float64)

    mean = w.mean()
    sd = w.std(ddof=1) if len(w) > 1 else np.nan

    return mean, sd

def _format_mean_sd(s: pd.Series, scale: float = 1.0, decimals: int = 1) -> str:
    """
    Return formatted 'mean\\n(sd)' string after winsorization.
    """
    s = _winsorize_series(s)
    if s.empty:
        return ""
    mean_val = s.mean() * scale
    sd_val = s.std(ddof=1) * scale
    return f"{mean_val:.{decimals}f}\n({sd_val:.{decimals}f})"

from scipy.stats.mstats import winsorize

GSIB_IDS_Q1_2022 = [
    934329, 488318, 212465, 449038, 476810, 3382547, 852218, 651448, 480228,
    1443266, 413208, 3357620, 1015560, 2980209, 214807, 304913, 670560,
    2325882, 2182786, 3066025, 398668, 541101, 229913, 1456501, 2489805,
    722777, 35301, 93619, 352745, 812164, 925411, 3212149, 451965, 688079,
    1225761, 2362458, 2531991,
]


def _winsorized_mean_sd_pct(subdf: pd.DataFrame, cols: list[str]) -> tuple[pd.Series, pd.Series]:
    ratios = subdf[cols].div(subdf["Total Asset"], axis=0) * 100
    ratios = ratios.astype("float64")  # convert from pandas Float64Dtype to NumPy float64

    mean = {}
    sd = {}

    for c in cols:
        x = ratios[c].to_numpy(dtype=np.float64, na_value=np.nan)
        x = x[~np.isnan(x)]

        if len(x) == 0:
            mean[c] = np.nan
            sd[c] = np.nan
            continue

        w = winsorize(x, limits=[0.05, 0.05])
        w = np.asarray(w, dtype=np.float64)

        mean[c] = w.mean()
        sd[c] = w.std(ddof=1) if len(w) > 1 else np.nan

    return pd.Series(mean), pd.Series(sd)


def calc_table_a1(bank_asset: pd.DataFrame) -> pd.DataFrame:
    """
    Paper-consistent Table A1 using the available asset rows.

    Columns:
    Aggregate, Full Sample, Full Sample (sd), Small, Small (sd),
    Large (non-GSIB), Large (sd), GSIB, GSIB (sd)
    """
    df = bank_asset.copy()

    required_cols = [
        "bank_id",
        "Total Asset",
        "size_category",
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
    asset_rows = [
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
    
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"calc_table_a1 missing required columns: {missing}")

    value_cols = [c for c in required_cols if c not in ["bank_id", "size_category"]]
    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[df["Total Asset"] > 0].copy()

    rows = ["Total Asset $", "Number of Banks"] + asset_rows

    out = pd.DataFrame(
        index=rows,
        columns=[
            "Aggregate",
            "Full sample(mean)", "Full sample(sd)",
            "small(mean)", "small(sd)",
            "large(mean)", "large(sd)",
            "GSIB(mean)", "GSIB(sd)",
        ],
        dtype="float64",
    )

    

    total_assets = df["Total Asset"]
    total_assets_sum = total_assets.sum()

    # Aggregate
    out.loc["Total Asset $", "Aggregate"] = total_assets_sum
    out.loc["Number of Banks", "Aggregate"] = len(df)

    for r in asset_rows:
        out.loc[r, "Aggregate"] = (
            df[r].sum() / total_assets_sum * 100 if total_assets_sum > 0 else np.nan
        )

    # ratios
    ratios = df.copy()
    denom = ratios["Total Asset"].replace(0, np.nan)

    for r in asset_rows:
        ratios[r] = 100 * ratios[r] / denom

    def fill_group(sub, mean_col, sd_col):

        for r in asset_rows:
            m, s = _winsorized_mean_sd(sub[r])
            out.loc[r, mean_col] = m
            out.loc[r, sd_col] = s

        m, s = _winsorized_mean_sd(sub["Total Asset"])
        out.loc["Total Asset $", mean_col] = m
        out.loc["Total Asset $", sd_col] = s

        out.loc["Number of Banks", mean_col] = len(sub)
        out.loc["Number of Banks", sd_col] = np.nan

    fill_group(ratios, "Full sample(mean)", "Full sample(sd)")
    fill_group(ratios[ratios["size_category"] == "Small"], "small(mean)", "small(sd)")
    fill_group(ratios[ratios["size_category"] == "Large non-GSIB"], "large(mean)", "large(sd)")
    fill_group(ratios[ratios["size_category"] == "GSIB"], "GSIB(mean)", "GSIB(sd)")

    return out.round(1)

def calc_table_a1_panel_b(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "bank_id",
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

    ratio_items = [
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
    result = pd.DataFrame(
        index=ratio_items,
        columns=[
            "Aggregate",
            "Full sample(mean)", "Full sample(sd)",
            "small(mean)", "small(sd)",
            "large(mean)", "large(sd)",
            "GSIB(mean)", "GSIB(sd)",
        ],
        dtype="float64",
    )
    raw = df.copy()

    denom = raw["Total Asset"].replace(0, np.nan)

    ratios = raw.copy()
    for r in ratio_items:
        ratios[r] = 100 * ratios[r] / denom

    total_assets_sum = raw["Total Asset"].sum()

    # aggregate
    for r in ratio_items:
        result.loc[r, "Aggregate"] = (
            raw[r].sum() / total_assets_sum * 100 if total_assets_sum > 0 else np.nan
        )

    def fill_group(sub, mean_col, sd_col):

        for r in ratio_items:
            m, s = _winsorized_mean_sd(sub[r])
            result.loc[r, mean_col] = m
            result.loc[r, sd_col] = s

    fill_group(ratios, "Full sample(mean)", "Full sample(sd)")
    fill_group(ratios[ratios["size_category"] == "Small"], "small(mean)", "small(sd)")
    fill_group(ratios[ratios["size_category"] == "Large non-GSIB"], "large(mean)", "large(sd)")
    fill_group(ratios[ratios["size_category"] == "GSIB"], "GSIB(mean)", "GSIB(sd)")

    return result.round(1)

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
