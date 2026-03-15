"""Compute Table 1 statistics: MTM loss summary by bank size category.

This module takes already-computed per-bank loss inputs and 
aggregates per-bank MTM loss results from calc_mtm_losses.py into the
summary statistics shown in Table 1 of Jiang et al. (2024):
- Aggregate Loss
- Bank Level Loss
- Bank Level Loss Std
- Share RMBS
- Share Treasury and Other
- Share Residential Mortgage
- Share Other Loan
- Loss/Asset
- Uninsured Deposit/MM Asset
- Number of Banks

Statistics are reported for four groups:
  All Banks, Small, Large non-GSIB, GSIB

Usage
-----
    from calc_table1 import calc_table1
    table1 = calc_table1(bank_losses, uninsured_ratio, insured_coverage)
"""

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Data prep helpers
# ---------------------------------------------------------------------------

def _prepare_bank_losses(bank_losses: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns needed for Table 1 summary statistics.
 
    Computes absolute total loss, per-bank loss-share percentages for each
    asset class, and loss-over-assets in percent.
 
    Parameters
    ----------
    bank_losses : pd.DataFrame
        Output of calc_mtm_losses.calc_bank_losses(). Must contain
        total_loss, rmbs_loss, treasury_loss, loans_loss, other_loans_loss,
        and loss_over_assets columns.
 
    Returns
    -------
    pd.DataFrame
        Copy of input with added columns: _abs_total_loss,
        Share RMBS (%), Share Treasury and Other (%),
        Share Residential Mortgage (%), Share Other Loan (%),
        Loss/Asset (%).
    """
    df = bank_losses.copy()

    # Absolute MTM loss magnitudes for bank-level summary rows
    df["_abs_total_loss"] = df["total_loss"].abs()

    # Asset-class loss shares used in notebook table_1
    loss_rmbs = df["rmbs_loss"].abs()
    loss_treasury = df["treasury_loss"].abs()
    loss_loans = df["loans_loss"].abs()
    loss_other = df["other_loans_loss"].abs()

    total_component_loss = loss_rmbs + loss_treasury + loss_loans + loss_other

    df["Share RMBS (%)"] = 100 * loss_rmbs / total_component_loss
    df["Share Treasury and Other (%)"] = 100 * loss_treasury / total_component_loss
    df["Share Residential Mortgage (%)"] = 100 * loss_loans / total_component_loss
    df["Share Other Loan (%)"] = 100 * loss_other / total_component_loss

    # Loss/Asset row in percent
    df["Loss/Asset (%)"] = df["loss_over_assets"].abs() * 100

    return df


 
def _insured_ratio_series(
    insured_coverage: pd.DataFrame,
    bank_ids: set[int],
) -> pd.Series:
    """Compute insured deposits / MTM assets (%) for a subset of banks.
 
    Parameters
    ----------
    insured_coverage : pd.DataFrame
        Output of calc_mtm_losses.calc_insured_deposit_coverage().
        Must contain bank_id, insured_deposits, mtm_assets.
    bank_ids : set[int]
        Bank IDs to include.
 
    Returns
    -------
    pd.Series
        Insured deposit ratio in percent.
    """
    sub = insured_coverage[insured_coverage["bank_id"].isin(bank_ids)].copy()
    return (sub["insured_deposits"].astype(float) / sub["mtm_assets"].astype(float)) * 100

def _uninsured_ratio_series(uninsured_ratio: pd.DataFrame, bank_ids: set[int]) -> pd.Series:
    """Extract uninsured deposits / MTM assets (%) for a subset of banks.
 
    Parameters
    ----------
    uninsured_ratio : pd.DataFrame
        Output of calc_mtm_losses.calc_uninsured_deposit_ratio().
        Must contain bank_id and uninsured_over_mtm_assets.
    bank_ids : set[int]
        Bank IDs to include.
 
    Returns
    -------
    pd.Series
        Uninsured deposit ratio in percent.
    """
    sub = uninsured_ratio[uninsured_ratio["bank_id"].isin(bank_ids)].copy()
    return sub["uninsured_over_mtm_assets"].astype(float) * 100
 
    
# ---------------------------------------------------------------------------
# Per-group statistics
# ---------------------------------------------------------------------------

def _group_stats(bank_losses, uninsured_ratio, insured_coverage, mask=None, label="All Banks"):
    """Compute Table 1 summary statistics for a subset of banks.

    Parameters
    ----------
    bank_losses : pd.DataFrame
        Output of calc_mtm_losses.calc_bank_losses().
    uninsured_ratio : pd.DataFrame
        Output of calc_mtm_losses.calc_uninsured_deposit_ratio().
    insured_coverage : pd.DataFrame
        Output of calc_mtm_losses.calc_insured_deposit_coverage().
    mask : pd.Series of bool, optional
        Row mask applied to bank_losses (and matched rows in other DFs).
        If None, uses all rows.
    label : str
        Row label in the output.

    Returns
    -------
    pd.Series
    """
    if mask is None:
        mask = pd.Series(True, index=bank_losses.index)
    else:
        mask = mask.reindex(bank_losses.index, fill_value=False)

    bl = bank_losses.loc[mask].copy()
    bank_ids = set(bl["bank_id"])

    ur = _uninsured_ratio_series(uninsured_ratio, bank_ids)

    return pd.Series(
        {
            "Aggregate Loss": round(bl["_abs_total_loss"].sum() / 1e6, 1),
            "Bank Level Loss": round(bl["_abs_total_loss"].median() / 1e3, 1),
            "Bank Level Loss Std": round(bl["_abs_total_loss"].std() / 1e6, 1),
            "Share RMBS": round(bl["Share RMBS (%)"].median(), 1),
            "Share RMBS Std": round(bl["Share RMBS (%)"].std(), 1),
            "Share Treasury and Other": round(
                bl["Share Treasury and Other (%)"].median(), 1
            ),
            "Share Treasury and Other Std": round(
                bl["Share Treasury and Other (%)"].std(), 1
            ),
            "Share Residential Mortgage": round(
                bl["Share Residential Mortgage (%)"].median(), 1
            ),
            "Share Residential Mortgage Std": round(
                bl["Share Residential Mortgage (%)"].std(), 1
            ),
            "Share Other Loan": round(bl["Share Other Loan (%)"].median(), 1),
            "Share Other Loan Std": round(bl["Share Other Loan (%)"].std(), 1),
            "Loss/Asset": round(bl["Loss/Asset (%)"].median(), 1),
            "Loss/Asset Std": round(bl["Loss/Asset (%)"].std(), 1),
            "Uninsured Deposit/MM Asset": round(ur.median(), 1),
            "Uninsured Deposit/MM Asset Std": round(ur.std(), 1),
            #"Insured Deposit/MM Asset": round(ic.median(), 1),
            #"Insured Deposit/MM Asset Std": round(ic.std(), 1),
            "Number of Banks": int(len(bl)),
        },
        name=label,
    )

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def calc_table1(
    bank_losses: pd.DataFrame,
    uninsured_ratio: pd.DataFrame,
    insured_coverage: pd.DataFrame,
) -> pd.DataFrame:
    """Assemble Table 1 for all bank groups.

    Parameters
    ----------
    bank_losses : pd.DataFrame
        Per-bank MTM loss output with at least:
        ``bank_id``, ``size_category``, ``total_loss``, ``rmbs_loss``,
        ``treasury_loss``, ``loans_loss``, ``other_loans_loss``,
        ``loss_over_assets``.
    uninsured_ratio : pd.DataFrame
        Per-bank uninsured deposits / MTM assets ratio.
    insured_coverage : pd.DataFrame
        Per-bank insured deposit coverage ratio.

    Returns
    -------
    pd.DataFrame
        Notebook-style Table 1 with rows = statistics and columns = bank groups.
    """
    bl = _prepare_bank_losses(bank_losses)

    groups = {
        "All Banks": pd.Series(True, index=bl.index),
        "Small": bl["size_category"] == "Small",
        "Large non-GSIB": bl["size_category"] == "Large non-GSIB",
        "GSIB": bl["size_category"] == "GSIB",
    }

    columns = {
        label: _group_stats(bl, uninsured_ratio, insured_coverage, mask=mask, label=label)
        for label, mask in groups.items()
    }
    return pd.DataFrame(columns)