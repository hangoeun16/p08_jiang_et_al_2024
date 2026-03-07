"""Unit tests for calc_mtm_losses.py.

Tests the MTM loss computation logic using synthetic inputs that mimic
the structure of real WRDS Call Report data.
"""

import numpy as np
import pandas as pd
import pytest

from calc_mtm_losses import (
    GSIB_IDS,
    LARGE_THRESHOLD,
    calc_bank_losses,
    calc_insured_deposit_coverage,
    calc_rmbs_multiplier,
    calc_price_changes,
    calc_uninsured_deposit_ratio,
    classify_banks,
)


# ---------------------------------------------------------------------------
# classify_banks
# ---------------------------------------------------------------------------


def test_classify_banks_gsib():
    """GSIB bank IDs should be classified as 'GSIB' regardless of asset size."""
    df = pd.DataFrame({
        "bank_id": [GSIB_IDS[0], GSIB_IDS[1]],
        "bank_name": ["GSIB A", "GSIB B"],
        "total_assets": [5_000_000, 2_000_000],
    })
    result = classify_banks(df)
    assert (result["size_category"] == "GSIB").all()


def test_classify_banks_large_non_gsib():
    """Banks above threshold and not GSIB should be 'Large non-GSIB'."""
    df = pd.DataFrame({
        "bank_id": [9999990, 9999991],
        "bank_name": ["Large A", "Large B"],
        "total_assets": [LARGE_THRESHOLD + 1, 10_000_000],
    })
    result = classify_banks(df)
    assert (result["size_category"] == "Large non-GSIB").all()


def test_classify_banks_small():
    """Banks at or below threshold and not GSIB should be 'Small'."""
    df = pd.DataFrame({
        "bank_id": [9999992, 9999993],
        "bank_name": ["Small A", "Small B"],
        "total_assets": [LARGE_THRESHOLD, 100_000],
    })
    result = classify_banks(df)
    assert (result["size_category"] == "Small").all()


# ---------------------------------------------------------------------------
# calc_rmbs_multiplier
# ---------------------------------------------------------------------------


def _make_etf_quarterly(mbs_prices, idx_prices, start_date="2022-03-31", end_date="2023-03-31"):
    """Helper: build a minimal quarterly ETF DataFrame."""
    dates = [pd.Timestamp(start_date), pd.Timestamp(end_date)]
    return pd.DataFrame(
        {
            "MBS ETF": mbs_prices,
            "SP Treasury Index": idx_prices,
            "iShares 0-1": [100, 98],
            "iShares 1-3": [100, 94],
            "sp 3-5": [100, 90],
            "iShares 7-10": [100, 80],
            "iShares 10-20": [100, 73],
            "iShares 20+": [100, 70],
        },
        index=pd.DatetimeIndex(dates, name="date"),
    )


def test_rmbs_multiplier_value():
    """RMBS multiplier = ΔiShares MBS ETF / ΔS&P Treasury Bond Index."""
    # MBS drops 8%, Treasury index drops 10% → multiplier = 0.08/0.10 = 0.80
    etf = _make_etf_quarterly([100, 92], [100, 90])
    mult = calc_rmbs_multiplier(etf, "2022-03-31", "2023-03-31")
    assert abs(mult - 0.8) < 1e-6


def test_rmbs_multiplier_greater_than_zero():
    """Multiplier should be positive when both MBS and Treasury prices fall."""
    etf = _make_etf_quarterly([100, 90], [100, 85])
    mult = calc_rmbs_multiplier(etf, "2022-03-31", "2023-03-31")
    assert mult > 0


# ---------------------------------------------------------------------------
# calc_price_changes
# ---------------------------------------------------------------------------


def test_price_changes_all_buckets():
    """calc_price_changes should return a key for every maturity bucket."""
    from calc_mtm_losses import BUCKET_TO_ETF
    etf = _make_etf_quarterly([100, 90], [100, 85])
    changes = calc_price_changes(etf, "2022-03-31", "2023-03-31")
    for bucket in BUCKET_TO_ETF:
        assert bucket in changes, f"Missing bucket: {bucket}"


def test_price_changes_sign():
    """Price changes should be negative when ETF prices fall."""
    etf = _make_etf_quarterly([100, 90], [100, 85])
    changes = calc_price_changes(etf, "2022-03-31", "2023-03-31")
    for bucket, chg in changes.items():
        assert chg < 0, f"Expected negative price change for {bucket}, got {chg}"


# ---------------------------------------------------------------------------
# calc_bank_losses
# ---------------------------------------------------------------------------


def _make_holdings_df(bank_id, bank_name, bucket_values):
    """Helper: create a single-row holdings DataFrame."""
    from calc_mtm_losses import BUCKET_COLS
    row = {"bank_id": bank_id, "bank_name": bank_name}
    row.update(dict(zip(BUCKET_COLS, bucket_values)))
    return pd.DataFrame([row])


def test_bank_losses_zero_holdings():
    """Bank with zero holdings should have zero total loss."""
    total_assets = pd.DataFrame({
        "bank_id": [1], "bank_name": ["Bank A"],
        "total_assets": [1_000_000], "size_category": ["Small"],
    })
    rmbs = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])
    loans = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])
    treasury = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])
    other = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])

    price_changes = {k: -0.10 for k in ["<3m", "3m-1y", "1y-3y", "3y-5y", "5y-15y", ">15y"]}

    result = calc_bank_losses(rmbs, loans, treasury, other, total_assets, price_changes, 0.8)
    assert len(result) == 1
    assert result.iloc[0]["total_loss"] == 0.0


def test_bank_losses_treasury_only():
    """Treasury loss = holdings × price_change (no multiplier)."""
    total_assets = pd.DataFrame({
        "bank_id": [1], "bank_name": ["Bank A"],
        "total_assets": [1_000_000], "size_category": ["Small"],
    })
    # Only >15y treasury holding of 100,000 ($thousands)
    empty = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])
    treasury = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 100_000])

    price_changes = {
        "<3m": 0, "3m-1y": 0, "1y-3y": 0, "3y-5y": 0, "5y-15y": 0, ">15y": -0.30
    }

    result = calc_bank_losses(empty, empty, treasury, empty, total_assets, price_changes, 0.8)
    expected_treasury_loss = 100_000 * (-0.30)  # = -30,000
    assert abs(result.iloc[0]["treasury_loss"] - expected_treasury_loss) < 1e-6


def test_bank_losses_rmbs_uses_multiplier():
    """RMBS loss should use the RMBS multiplier."""
    total_assets = pd.DataFrame({
        "bank_id": [1], "bank_name": ["Bank A"],
        "total_assets": [1_000_000], "size_category": ["Small"],
    })
    rmbs = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 100_000])
    empty = _make_holdings_df(1, "Bank A", [0, 0, 0, 0, 0, 0])

    price_changes = {
        "<3m": 0, "3m-1y": 0, "1y-3y": 0, "3y-5y": 0, "5y-15y": 0, ">15y": -0.30
    }
    multiplier = 0.8

    result = calc_bank_losses(rmbs, empty, empty, empty, total_assets, price_changes, multiplier)
    expected_rmbs_loss = 100_000 * multiplier * (-0.30)  # = -24,000
    assert abs(result.iloc[0]["rmbs_loss"] - expected_rmbs_loss) < 1e-6


# ---------------------------------------------------------------------------
# Aggregate totals test against paper (Table 1)
# ---------------------------------------------------------------------------


def test_aggregate_loss_order_of_magnitude():
    """Aggregate loss should be in the trillions range (~$2T for all banks)."""
    from settings import config
    from pathlib import Path

    data_dir = Path(config("DATA_DIR"))
    parquet_path = data_dir / "bank_losses.parquet"

    if not parquet_path.exists():
        pytest.skip("bank_losses.parquet not found — run run_analysis.py first")

    bank_losses = pd.read_parquet(parquet_path)
    agg_loss_billions = -bank_losses["total_loss"].sum() / 1e6  # $thousands → $billions
    # Paper reports ~$2.2T aggregate loss
    assert 1_000 < agg_loss_billions < 4_000, (
        f"Aggregate loss ${agg_loss_billions:.0f}B outside expected range $1T-$4T"
    )


def test_bank_count_all():
    """Total bank count should be approximately 4,844 (Table 1, paper)."""
    from settings import config
    from pathlib import Path

    data_dir = Path(config("DATA_DIR"))
    parquet_path = data_dir / "bank_losses.parquet"

    if not parquet_path.exists():
        pytest.skip("bank_losses.parquet not found — run run_analysis.py first")

    bank_losses = pd.read_parquet(parquet_path)
    count = len(bank_losses)
    assert 4_000 < count < 6_000, f"Bank count {count} outside expected range 4000-6000"
