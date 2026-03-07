"""Unit tests for clean_data.py.

Tests that each extraction function correctly filters to the report date,
renames columns, and handles missing data appropriately. Uses synthetic
mini-DataFrames that mimic the structure of WRDS Call Report data.
"""

import numpy as np
import pandas as pd
import pytest

from clean_data import (
    BUCKET_COLS,
    get_rmbs,
    get_treasuries,
    get_loans,
    get_other_loans,
    get_total_assets,
    get_uninsured_deposits,
    get_insured_deposits,
    clean_etf_prices,
)

REPORT_DATE = "2022-03-31"


def _make_rcfd1(bank_ids, report_dates, rmbs_vals=None, loan_vals=None, other_loan_vals=None):
    """Build a minimal RCFD series 1 DataFrame for testing."""
    n = len(bank_ids)
    vals = rmbs_vals if rmbs_vals else [[0] * 6] * n
    loan = loan_vals if loan_vals else [[0] * 6] * n
    other = other_loan_vals if other_loan_vals else [[0] * 6] * n
    rows = []
    for i, (bid, rdate, v, lv, ov) in enumerate(zip(bank_ids, report_dates, vals, loan, other)):
        row = {
            "rssd9001": bid, "rssd9017": f"Bank {bid}", "rssd9999": pd.Timestamp(rdate),
            "rcfda555": v[0], "rcfda556": v[1], "rcfda557": v[2],
            "rcfda558": v[3], "rcfda559": v[4], "rcfda560": v[5],
            "rcfda564": lv[0], "rcfda565": lv[1], "rcfda566": lv[2],
            "rcfda567": lv[3], "rcfda568": lv[4], "rcfda569": lv[5],
            "rcfda570": ov[0], "rcfda571": ov[1], "rcfda572": ov[2],
            "rcfda573": ov[3], "rcfda574": ov[4], "rcfda575": ov[5],
            "rcfd0010": 50000,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def _make_rcon1(bank_ids, report_dates, rmbs_vals=None, loan_vals=None):
    """Build a minimal RCON series 1 DataFrame for testing.

    Mirrors actual WRDS wrds_call_rcon_1 columns: only rcona555 (<3m) and
    rcona557 (1y-3y) for RMBS; rcona556/558-560 live in rcon_2.
    """
    n = len(bank_ids)
    vals = rmbs_vals if rmbs_vals else [[0] * 6] * n
    loans = loan_vals if loan_vals else [[0] * 6] * n
    rows = []
    for bid, rdate, v, lv in zip(bank_ids, report_dates, vals, loans):
        row = {
            "rssd9001": bid, "rssd9017": f"Bank {bid}", "rssd9999": pd.Timestamp(rdate),
            "rcona555": v[0], "rcona557": v[2],   # only these 2 RMBS buckets in rcon_1
            "rcona564": lv[0], "rcona565": lv[1], "rcona566": lv[2],
            "rcona567": lv[3], "rcona568": lv[4], "rcona569": lv[5],
            "rcon5597": 10000, "rconf049": 5000, "rconf045": 2000,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def _make_rcfd2(bank_ids, report_dates, treasury_vals=None, total_assets=None):
    """Build a minimal RCFD series 2 DataFrame for testing."""
    n = len(bank_ids)
    vals = treasury_vals if treasury_vals else [[0] * 6] * n
    assets = total_assets if total_assets else [500000] * n
    rows = []
    for bid, rdate, v, ta in zip(bank_ids, report_dates, vals, assets):
        row = {
            "rssd9001": bid, "rssd9017": f"Bank {bid}", "rssd9999": pd.Timestamp(rdate),
            "rcfda549": v[0], "rcfda550": v[1], "rcfda551": v[2],
            "rcfda552": v[3], "rcfda553": v[4], "rcfda554": v[5],
            "rcfd2170": ta,
        }
        rows.append(row)
    return pd.DataFrame(rows)


def _make_rcon2(bank_ids, report_dates, treasury_vals=None, total_assets=None, rmbs_vals=None):
    """Build a minimal RCON series 2 DataFrame for testing.

    Mirrors actual WRDS wrds_call_rcon_2 columns: includes the 4 RMBS buckets
    (rcona556/558-560) that are absent from rcon_1.
    """
    n = len(bank_ids)
    vals = treasury_vals if treasury_vals else [[0] * 6] * n
    assets = total_assets if total_assets else [500000] * n
    rvals = rmbs_vals if rmbs_vals else [[0] * 4] * n  # 4 RMBS buckets: 3m-1y,3y-5y,5y-15y,>15y
    rows = []
    for bid, rdate, v, ta, rv in zip(bank_ids, report_dates, vals, assets, rvals):
        row = {
            "rssd9001": bid, "rssd9017": f"Bank {bid}", "rssd9999": pd.Timestamp(rdate),
            "rcona549": v[0], "rcona550": v[1], "rcona551": v[2],
            "rcona552": v[3], "rcona553": v[4], "rcona554": v[5],
            "rcona556": rv[0], "rcona558": rv[1], "rcona559": rv[2], "rcona560": rv[3],
            "rcona570": 0, "rcona571": 0, "rcona572": 0,
            "rcona573": 0, "rcona574": 0, "rcona575": 0,
            "rcon2170": ta,
        }
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_get_rmbs_date_filter():
    """get_rmbs should only return rows matching the report date."""
    rcfd1 = _make_rcfd1([1, 2], ["2022-03-31", "2021-12-31"],
                        rmbs_vals=[[1000, 0, 0, 0, 0, 0], [500, 0, 0, 0, 0, 0]])
    rcon1 = _make_rcon1([1, 2], ["2022-03-31", "2021-12-31"])
    rcon2 = _make_rcon2([1, 2], ["2022-03-31", "2021-12-31"])
    result = get_rmbs(rcfd1, rcon1, rcon2, REPORT_DATE)
    assert all(result["bank_id"].isin([1]))


def test_get_rmbs_column_names():
    """get_rmbs output should contain the standard bucket column names."""
    rcfd1 = _make_rcfd1([1], ["2022-03-31"])
    rcon1 = _make_rcon1([1], ["2022-03-31"])
    rcon2 = _make_rcon2([1], ["2022-03-31"])
    result = get_rmbs(rcfd1, rcon1, rcon2, REPORT_DATE)
    for col in BUCKET_COLS:
        assert col in result.columns, f"Missing column: {col}"


def test_get_total_assets_deduplication():
    """get_total_assets should keep highest total_assets per bank (no doubles)."""
    # Bank 1 appears in both RCFD (larger) and RCON (smaller)
    rcfd2 = _make_rcfd2([1], ["2022-03-31"], total_assets=[1_000_000])
    rcon2 = _make_rcon2([1], ["2022-03-31"], total_assets=[800_000])
    result = get_total_assets(rcfd2, rcon2, REPORT_DATE)
    assert len(result) == 1
    assert result.iloc[0]["total_assets"] == 1_000_000


def test_get_uninsured_deposits_values():
    """get_uninsured_deposits should return correct rcon5597 values."""
    rcon1 = _make_rcon1([1, 2], ["2022-03-31", "2022-03-31"])
    result = get_uninsured_deposits(rcon1, REPORT_DATE)
    assert len(result) == 2
    assert (result["uninsured_deposits"] == 10000).all()


def test_get_insured_deposits_sum():
    """Insured deposits should be rconf049 + rconf045."""
    rcon1 = _make_rcon1([1], ["2022-03-31"])
    result = get_insured_deposits(rcon1, REPORT_DATE)
    assert result.iloc[0]["insured_deposits"] == 7000  # 5000 + 2000


def test_clean_etf_prices_quarterly():
    """clean_etf_prices should downsample to quarterly and filter date range."""
    dates = pd.date_range("2022-01-01", "2023-06-30", freq="B")
    etf = pd.DataFrame(
        {"iShares 0-1": 100.0, "MBS ETF": 50.0, "SP Treasury Index": 200.0},
        index=pd.DatetimeIndex(dates, name="date"),
    )
    result = clean_etf_prices(etf, "2022-03-31", "2023-03-31")
    assert len(result) <= 5  # At most 5 quarters from Q1-2022 to Q1-2023
    assert result.index[0] <= pd.Timestamp("2022-03-31")
    assert result.index[-1] <= pd.Timestamp("2023-03-31")


def test_get_loans_no_dropna():
    """get_loans should include rows even when some bucket values are NaN."""
    rcon1 = _make_rcon1([1], ["2022-03-31"])
    # Introduce NaN in one bucket
    rcon1.loc[0, "rcona564"] = np.nan
    result = get_loans(rcon1, REPORT_DATE)
    assert len(result) == 1  # Row still present despite NaN
