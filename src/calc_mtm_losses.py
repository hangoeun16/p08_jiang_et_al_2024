"""Compute mark-to-market (MTM) losses and deposit ratios per Jiang et al. (2024).

Implements the MTM loss methodology from Section II of the paper:
  - Treasury and non-RMBS securities: price change derived from iShares Treasury ETFs
  - RMBS and first-lien residential mortgages: treasury price change × RMBS multiplier
    where multiplier = ΔiShares MBS ETF / ΔS&P Treasury Bond Index

Bank size classification (Jiang et al. Table 1):
  - GSIB: 17 globally systemically important banks (hardcoded list)
  - Large non-GSIB: total_assets > $1.384B and not GSIB
  - Small: total_assets <= $1.384B

Maturity bucket → ETF mapping used for price change computation:
  '<3m'   → 'iShares 0-1' (SHV)
  '3m-1y' → 'iShares 0-1' (SHV, grouped with <3m as '<1y')
  '1y-3y' → 'iShares 1-3' (SHY)
  '3y-5y' → 'sp 3-5' (IEI)
  '5y-15y'→ 'iShares 7-10' (IEF)
  '>15y'  → 'iShares 20+' (TLT)

Usage
-----
    from calc_mtm_losses import calc_bank_losses, classify_banks
"""

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# GSIB bank IDs (WRDS rssd9001 identifiers)
# Source: Federal Reserve list of G-SIBs as of 2022
# ---------------------------------------------------------------------------
GSIB_IDS = [
    852218,   # JPMorgan Chase
    480228,   # Bank of America
    476810,   # Citigroup
    413208,   # HSBC North America
    2980209,  # Barclays US
    2182786,  # Goldman Sachs
    541101,   # Bank of New York Mellon
    655839,   # CCB Community Bank
    1015560,  # ICBC Americas
    229913,   # Mizuho
    1456501,  # Morgan Stanley
    722777,   # Santander BancorpUSA
    35301,    # State Street
    925411,   # Sumitomo Mitsui
    497404,   # TD Bank US Holding
    3212149,  # UBS Americas
    451965,   # Wells Fargo
]

# Threshold for large vs. small classification ($1.384B in thousands)
LARGE_THRESHOLD = 1_384_000  # $1.384B in $thousands

# Maturity buckets (from clean_data.py)
BUCKET_COLS = ["<3m", "3m-1y", "1y-3y", "3y-5y", "5y-15y", ">15y"]

# Maps each maturity bucket to the ETF column used for price change
BUCKET_TO_ETF = {
    "<3m":    "iShares 0-1",
    "3m-1y":  "iShares 0-1",
    "1y-3y":  "iShares 1-3",
    "3y-5y":  "sp 3-5",
    "5y-15y": "iShares 7-10",
    ">15y":   "iShares 20+",
}


def classify_banks(total_assets_df):
    """Add a 'size_category' column to the total assets DataFrame.

    Parameters
    ----------
    total_assets_df : pd.DataFrame
        Output of clean_data.get_total_assets(). Must have 'bank_id' and
        'total_assets' columns.

    Returns
    -------
    pd.DataFrame
        Same as input with added 'size_category' column:
        'GSIB', 'Large non-GSIB', or 'Small'.
    """
    df = total_assets_df.copy()

    def _classify(row):
        if row["bank_id"] in GSIB_IDS:
            return "GSIB"
        elif row["total_assets"] > LARGE_THRESHOLD:
            return "Large non-GSIB"
        else:
            return "Small"

    df["size_category"] = df.apply(_classify, axis=1)
    return df


def calc_rmbs_multiplier(etf_quarterly, start_date, end_date):
    """Compute the RMBS multiplier = ΔiShares MBS ETF / ΔS&P Treasury Bond Index.

    Parameters
    ----------
    etf_quarterly : pd.DataFrame
        Quarterly ETF prices from clean_data.clean_etf_prices(). Must contain
        'MBS ETF' and 'SP Treasury Index' columns.
    start_date : str
        Start date string, e.g. '2022-03-31'.
    end_date : str
        End date string, e.g. '2023-03-31'.

    Returns
    -------
    float
        RMBS multiplier scalar.
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    mbs_start = etf_quarterly.loc[start_ts, "MBS ETF"]
    mbs_end = etf_quarterly.loc[end_ts, "MBS ETF"]
    idx_start = etf_quarterly.loc[start_ts, "SP Treasury Index"]
    idx_end = etf_quarterly.loc[end_ts, "SP Treasury Index"]

    mbs_change = (mbs_end / mbs_start) - 1.0
    idx_change = (idx_end / idx_start) - 1.0

    return mbs_change / idx_change


def calc_price_changes(etf_quarterly, start_date, end_date):
    """Compute percentage price changes per maturity bucket ETF.

    Parameters
    ----------
    etf_quarterly : pd.DataFrame
        Quarterly ETF prices. Must contain all columns in BUCKET_TO_ETF.values().
    start_date : str
    end_date : str

    Returns
    -------
    dict
        Mapping bucket label → fractional price change (negative = price drop).
    """
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    changes = {}
    for bucket, etf_col in BUCKET_TO_ETF.items():
        p_start = etf_quarterly.loc[start_ts, etf_col]
        p_end = etf_quarterly.loc[end_ts, etf_col]
        changes[bucket] = (p_end / p_start) - 1.0

    return changes


def _aggregate_by_bank(df):
    """Sum maturity bucket columns by bank_id, returning one row per bank."""
    available_buckets = [b for b in BUCKET_COLS if b in df.columns]
    return (
        df.groupby(["bank_id", "bank_name"])[available_buckets]
        .sum()
        .reset_index()
    )


def calc_bank_losses(
    rmbs_df,
    loans_df,
    treasury_df,
    other_loans_df,
    total_assets_df,
    price_changes,
    rmbs_multiplier,
):
    """Compute per-bank MTM losses for all four asset categories.

    For RMBS and first-lien mortgages:
        loss = Σ_bucket (holdings_bucket × rmbs_multiplier × price_change_bucket)

    For treasury/other securities and other loans:
        loss = Σ_bucket (holdings_bucket × price_change_bucket)

    Parameters
    ----------
    rmbs_df : pd.DataFrame
        RMBS holdings from clean_data.get_rmbs().
    loans_df : pd.DataFrame
        First-lien mortgage holdings from clean_data.get_loans().
    treasury_df : pd.DataFrame
        Treasury/other securities holdings from clean_data.get_treasuries().
    other_loans_df : pd.DataFrame
        Other loan holdings from clean_data.get_other_loans().
    total_assets_df : pd.DataFrame
        Total assets from clean_data.get_total_assets(), with 'size_category'
        added by classify_banks().
    price_changes : dict
        Fractional price changes per bucket from calc_price_changes().
    rmbs_multiplier : float
        RMBS multiplier from calc_rmbs_multiplier().

    Returns
    -------
    pd.DataFrame
        One row per bank with columns:
        bank_id, bank_name, size_category, total_assets,
        rmbs_loss, loans_loss, treasury_loss, other_loans_loss, total_loss,
        rmbs_assets, loans_assets, treasury_assets, other_loans_assets,
        loss_over_assets
    """
    # Aggregate each asset type to one row per bank
    rmbs_agg = _aggregate_by_bank(rmbs_df)
    loans_agg = _aggregate_by_bank(loans_df)
    treasury_agg = _aggregate_by_bank(treasury_df)
    other_loans_agg = _aggregate_by_bank(other_loans_df)

    results = []
    for _, asset_row in total_assets_df.iterrows():
        bid = asset_row["bank_id"]
        bname = asset_row["bank_name"]
        total_assets = asset_row["total_assets"]
        size_cat = asset_row.get("size_category", "Unknown")

        rmbs_loss = loans_loss = treasury_loss = other_loans_loss = 0.0
        rmbs_assets = loans_assets = treasury_assets = other_loans_assets = 0.0

        def _get_row(agg_df):
            rows = agg_df[agg_df["bank_id"] == bid]
            return rows.iloc[0] if not rows.empty else None

        # RMBS losses (with RMBS multiplier)
        rmbs_row = _get_row(rmbs_agg)
        if rmbs_row is not None:
            for bucket in BUCKET_COLS:
                if bucket in rmbs_row.index and not pd.isna(rmbs_row[bucket]):
                    amt = rmbs_row[bucket]
                    rmbs_loss += amt * rmbs_multiplier * price_changes[bucket]
                    rmbs_assets += amt

        # First-lien mortgage losses (with RMBS multiplier)
        loans_row = _get_row(loans_agg)
        if loans_row is not None:
            for bucket in BUCKET_COLS:
                if bucket in loans_row.index and not pd.isna(loans_row[bucket]):
                    amt = loans_row[bucket]
                    loans_loss += amt * rmbs_multiplier * price_changes[bucket]
                    loans_assets += amt

        # Treasury losses (direct price change, no multiplier)
        treasury_row = _get_row(treasury_agg)
        if treasury_row is not None:
            for bucket in BUCKET_COLS:
                if bucket in treasury_row.index and not pd.isna(treasury_row[bucket]):
                    amt = treasury_row[bucket]
                    treasury_loss += amt * price_changes[bucket]
                    treasury_assets += amt

        # Other loan losses (direct price change, no multiplier)
        other_loans_row = _get_row(other_loans_agg)
        if other_loans_row is not None:
            for bucket in BUCKET_COLS:
                if bucket in other_loans_row.index and not pd.isna(other_loans_row[bucket]):
                    amt = other_loans_row[bucket]
                    other_loans_loss += amt * price_changes[bucket]
                    other_loans_assets += amt

        total_loss = rmbs_loss + loans_loss + treasury_loss + other_loans_loss
        loss_over_assets = (-total_loss / total_assets) if total_assets > 0 else np.nan

        results.append({
            "bank_id": bid,
            "bank_name": bname,
            "size_category": size_cat,
            "total_assets": total_assets,
            "rmbs_loss": rmbs_loss,
            "loans_loss": loans_loss,
            "treasury_loss": treasury_loss,
            "other_loans_loss": other_loans_loss,
            "total_loss": total_loss,
            "rmbs_assets": rmbs_assets,
            "loans_assets": loans_assets,
            "treasury_assets": treasury_assets,
            "other_loans_assets": other_loans_assets,
            "loss_over_assets": loss_over_assets,
        })

    return pd.DataFrame(results)


def calc_uninsured_deposit_ratio(uninsured_df, bank_losses_df):
    """Compute uninsured deposits / mark-to-market assets for each bank.

    MTM assets = book value total_assets + total_loss (loss is negative).

    Parameters
    ----------
    uninsured_df : pd.DataFrame
        From clean_data.get_uninsured_deposits(). Columns: bank_id, uninsured_deposits.
    bank_losses_df : pd.DataFrame
        From calc_bank_losses(). Must have bank_id, total_assets, total_loss.

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, total_assets, total_loss, mtm_assets,
        uninsured_deposits, uninsured_over_mtm_assets
    """
    uninsured_lookup = uninsured_df.set_index("bank_id")["uninsured_deposits"].fillna(0).to_dict()

    rows = []
    for _, row in bank_losses_df.iterrows():
        bid = row["bank_id"]
        mtm_assets = row["total_assets"] + row["total_loss"]
        uninsured = uninsured_lookup.get(bid, 0) or 0
        ratio = (uninsured / mtm_assets) if mtm_assets > 0 else np.nan
        rows.append({
            "bank_id": bid,
            "total_assets": row["total_assets"],
            "total_loss": row["total_loss"],
            "mtm_assets": mtm_assets,
            "uninsured_deposits": uninsured,
            "uninsured_over_mtm_assets": ratio,
        })

    return pd.DataFrame(rows)


def calc_insured_deposit_coverage(insured_df, uninsured_df, bank_losses_df):
    """Compute insured deposit coverage ratio for each bank.

    Coverage = (MTM assets - uninsured deposits - insured deposits) / insured deposits

    Parameters
    ----------
    insured_df : pd.DataFrame
        From clean_data.get_insured_deposits(). Columns: bank_id, insured_deposits.
    uninsured_df : pd.DataFrame
        From clean_data.get_uninsured_deposits(). Columns: bank_id, uninsured_deposits.
    bank_losses_df : pd.DataFrame
        From calc_bank_losses().

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, mtm_assets, insured_deposits, uninsured_deposits,
        insured_deposit_coverage
    """
    insured_lookup = insured_df.set_index("bank_id")["insured_deposits"].fillna(0).to_dict()
    uninsured_lookup = uninsured_df.set_index("bank_id")["uninsured_deposits"].fillna(0).to_dict()

    rows = []
    for _, row in bank_losses_df.iterrows():
        bid = row["bank_id"]
        mtm_assets = row["total_assets"] + row["total_loss"]
        insured = insured_lookup.get(bid, 0) or 0
        uninsured = uninsured_lookup.get(bid, 0) or 0
        coverage = ((mtm_assets - uninsured - insured) / insured) if insured > 0 else np.nan
        rows.append({
            "bank_id": bid,
            "mtm_assets": mtm_assets,
            "insured_deposits": insured,
            "uninsured_deposits": uninsured,
            "insured_deposit_coverage": coverage,
        })

    return pd.DataFrame(rows)
