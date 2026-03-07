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
