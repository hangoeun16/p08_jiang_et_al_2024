"""Compute Table 1 statistics: MTM loss summary by bank size category.

Aggregates per-bank MTM loss results from calc_mtm_losses.py into the
summary statistics shown in Table 1 of Jiang et al. (2024):
  - Aggregate Loss (sum, in trillions)
  - Bank-Level Loss (median, in millions)
  - Median Loss/Asset (%)
  - Median Uninsured Deposits/MTM Assets (%)
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
        mask = pd.Series([True] * len(bank_losses), index=bank_losses.index)

    bl = bank_losses[mask]
    bid_set = set(bl["bank_id"])

    ur = uninsured_ratio[uninsured_ratio["bank_id"].isin(bid_set)]
    ic = insured_coverage[insured_coverage["bank_id"].isin(bid_set)]

    agg_loss_trillions = -bl["total_loss"].sum() / 1e9  # thousands → billions, then billions reported

    return pd.Series(
        {
            "Aggregate Loss ($B)": round(-bl["total_loss"].sum() / 1e6, 1),
            "Median Bank Loss ($M)": round(-bl["total_loss"].median() / 1e3, 1),
            "Median Loss/Assets (%)": round(bl["loss_over_assets"].median() * 100, 1),
            "Median Unins. Dep./MTM Assets (%)": round(
                ur["uninsured_over_mtm_assets"].median() * 100, 1
            ),
            "Number of Banks": len(bl),
        },
        name=label,
    )


def calc_table1(bank_losses, uninsured_ratio, insured_coverage):
    """Assemble Table 1 with statistics for all four bank size groups.

    Parameters
    ----------
    bank_losses : pd.DataFrame
        Per-bank losses from calc_mtm_losses.calc_bank_losses() with
        'size_category' column added by classify_banks().
    uninsured_ratio : pd.DataFrame
        From calc_mtm_losses.calc_uninsured_deposit_ratio().
    insured_coverage : pd.DataFrame
        From calc_mtm_losses.calc_insured_deposit_coverage().

    Returns
    -------
    pd.DataFrame
        Table 1 with rows = statistics, columns = bank groups.
    """
    groups = {
        "All Banks": None,
        "Small": bank_losses["size_category"] == "Small",
        "Large non-GSIB": bank_losses["size_category"] == "Large non-GSIB",
        "GSIB": bank_losses["size_category"] == "GSIB",
    }

    columns = {}
    for label, mask in groups.items():
        columns[label] = _group_stats(
            bank_losses, uninsured_ratio, insured_coverage, mask=mask, label=label
        )

    table1 = pd.DataFrame(columns)
    return table1
