"""Pull and cache ETF price data needed for MTM loss calculations.

Downloads daily adjusted closing prices for Treasury bond ETFs and the
iShares MBS ETF via yfinance over the configured date range. These prices
are used to compute percentage price changes from Q1 2022 to Q1 2023,
which serve as inputs to the mark-to-market loss methodology of
Jiang et al. (2024).

ETF tickers used:
  - SHV:  iShares Short Treasury Bond ETF (0-1 year)
  - SHY:  iShares 1-3 Year Treasury Bond ETF
  - IEI:  iShares 3-7 Year Treasury Bond ETF (proxy for 3-5 year bucket)
  - IEF:  iShares 7-10 Year Treasury Bond ETF
  - TLH:  iShares 10-20 Year Treasury Bond ETF
  - TLT:  iShares 20+ Year Treasury Bond ETF
  - MBB:  iShares MBS ETF (used for RMBS multiplier)
  - GOVT: iShares U.S. Treasury Bond ETF (proxy for S&P Treasury Bond Index)

Usage
-----
Run directly to pull and cache ETF prices:
    python pull_etf_data.py

Or import:
    from pull_etf_data import load_etf_data
"""

import pandas as pd
import yfinance as yf
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")

# Tickers and their descriptive column names
ETF_TICKERS = {
    "SHV": "iShares 0-1",
    "SHY": "iShares 1-3",
    "IEI": "sp 3-5",
    "IEF": "iShares 7-10",
    "TLH": "iShares 10-20",
    "TLT": "iShares 20+",
    "MBB": "MBS ETF",
    "GOVT": "SP Treasury Index",
}


def pull_etf_data(
    tickers=ETF_TICKERS,
    start_date=START_DATE,
    end_date=END_DATE,
):
    """Download daily adjusted closing prices for Treasury and MBS ETFs.

    Parameters
    ----------
    tickers : dict
        Mapping of yfinance ticker symbol → column name in output.
    start_date : str or datetime
        Start date for download.
    end_date : str or datetime
        End date for download.

    Returns
    -------
    pd.DataFrame
        Date-indexed DataFrame with one column per ETF (using descriptive names).
    """
    raw = yf.download(
        list(tickers.keys()),
        start=str(start_date)[:10],
        end=str(end_date)[:10],
        auto_adjust=True,
        progress=False,
    )
    # Extract Close prices and rename columns
    prices = raw["Close"].rename(columns=tickers)
    prices.index.name = "date"
    return prices


def load_etf_data(data_dir=DATA_DIR):
    """Load cached ETF price data from _data/etf_prices.parquet."""
    return pd.read_parquet(Path(data_dir) / "etf_prices.parquet")


if __name__ == "__main__":
    df = pull_etf_data()
    df.to_parquet(DATA_DIR / "etf_prices.parquet")
    print(f"ETF prices: {len(df)} rows, columns: {list(df.columns)}")
    print(df.tail())
