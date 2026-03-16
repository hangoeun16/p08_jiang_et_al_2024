"""Generate a summary table of ETF price changes used in the MTM methodology.

Loads ETF price data from _data/etf_prices.parquet, computes the percentage
price change from Q1 2022 to Q1 2023 for each ETF, and writes a LaTeX table
to _output/table_etf.tex.

Usage
-----
    python create_etf_table.py
"""

import pandas as pd
from pathlib import Path

from settings import config
import clean_data
import calc_mtm_losses

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")

# (ETF column name after yfinance rename, ticker symbol, maturity bucket label)
# The 5y-15y bucket uses a 70/30 blend of IEF and TLH; both are listed here
# so the table shows each ETF's individual price change for transparency.
ETF_ROWS = [
    ("iShares 0-1",       "SHV",  r"$<$3m, 3m--1y"),
    ("iShares 1-3",       "SHY",  r"1y--3y"),
    ("sp 3-5",            "IEI",  r"3y--5y"),
    ("iShares 7-10",      "IEF",  r"5y--15y (70\% weight)"),
    ("iShares 10-20",     "TLH",  r"5y--15y (30\% weight)"),
    ("iShares 20+",       "TLT",  r"$>$15y"),
    ("MBS ETF",           "MBB",  r"MBS multiplier (numerator)"),
    ("SP Treasury Index", "GOVT", r"Treasury index (denominator)"),
]


def load_etf_table_data(data_dir=DATA_DIR):
    """Load and quarterly-resample ETF prices.

    Parameters
    ----------
    data_dir : Path

    Returns
    -------
    pd.DataFrame
        Quarterly ETF prices indexed by quarter-end dates.
    """
    etf_raw = pd.read_parquet(Path(data_dir) / "etf_prices.parquet")
    return clean_data.clean_etf_prices(etf_raw, REPORT_DATE, MTM_END_DATE)


def format_etf_table_latex(etf_quarterly):
    """Convert quarterly ETF prices to a LaTeX tabular string.

    Parameters
    ----------
    etf_quarterly : pd.DataFrame
        Quarterly ETF prices with REPORT_DATE and MTM_END_DATE as index entries.

    Returns
    -------
    str
        LaTeX table source.
    """
    start_ts = pd.Timestamp(REPORT_DATE)
    end_ts = pd.Timestamp(MTM_END_DATE)
    rmbs_mult = calc_mtm_losses.calc_rmbs_multiplier(
        etf_quarterly, REPORT_DATE, MTM_END_DATE
    )

    caption = (
        r"Interest Rate Shock: iShares ETF Price Changes, Q1~2022 to Q1~2023. "
        r"The table reports adjusted closing prices for the seven ETFs used in the "
        r"mark-to-market loss calculation, sampled at the end of Q1~2022 and Q1~2023. "
        r"The key takeaway is that price declines are steeply increasing in duration: "
        r"the short-term SHV (0--1 year) fell by less than 1\%, while the long-term "
        r"TLT (20$+$ years) fell by more than 30\%. "
        r"This duration-dependent shock directly explains why banks that held "
        r"long-dated fixed-income securities suffered disproportionately large "
        r"mark-to-market losses relative to banks with shorter-duration portfolios. "
        r"The RMBS multiplier---the ratio of the MBS ETF price change (MBB) "
        r"to the broad Treasury index change (GOVT)---scales mortgage-related "
        r"losses to reflect the steeper decline in mortgage-backed security prices "
        rf"relative to Treasuries (computed value: ${rmbs_mult:.4f}$). "
        r"The 5y--15y maturity bucket uses a 70/30 blended price change of IEF "
        r"and TLH rather than IEF alone; because the WRDS bucket spans bonds from "
        r"5 to 15 years, IEF (7--10yr) understates losses for the longer-duration "
        r"end of the range, and a blend with TLH (10--20yr) provides a more accurate "
        r"effective duration proxy."
    )

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{" + caption + r"}",
        r"\label{tab:table_etf}",
        r"\small",
        r"\begin{tabular}{llrrr}",
        r"\toprule",
        r"Maturity Bucket & Ticker & Q1~2022 Price & Q1~2023 Price & Change (\%) \\",
        r"\midrule",
    ]

    for col, ticker, bucket in ETF_ROWS:
        p_start = etf_quarterly.loc[start_ts, col]
        p_end = etf_quarterly.loc[end_ts, col]
        pct_chg = (p_end / p_start - 1.0) * 100
        sign = "+" if pct_chg >= 0 else ""
        lines.append(
            rf"{bucket} & {ticker} & \${p_start:.2f} & \${p_end:.2f}"
            rf" & {sign}{pct_chg:.2f}\% \\"
        )

    lines += [
        r"\midrule",
        (
            r"\multicolumn{5}{l}{"
            rf"RMBS multiplier (MBB\,/\,GOVT price change ratio): ${rmbs_mult:.4f}$"
            r"} \\"
        ),
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]

    return "\n".join(lines)


def create_etf_table(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    """Load ETF data, format as LaTeX, and save to _output/table_etf.tex.

    Parameters
    ----------
    data_dir : Path
    output_dir : Path
    """
    etf_quarterly = load_etf_table_data(data_dir)
    latex_str = format_etf_table_latex(etf_quarterly)

    output_path = Path(output_dir) / "table_etf.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    create_etf_table()
