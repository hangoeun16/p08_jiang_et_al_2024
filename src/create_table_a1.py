"""Generate Table A1 (balance sheet composition) as a LaTeX file.

Loads pre-computed balance sheet data from _data/table_a1.parquet and
writes a LaTeX table to _output/table_a1.tex for inclusion in the report.

Usage
-----
    python create_table_a1.py
"""

import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")


def load_table_a1(data_dir=DATA_DIR):
    """Load Table A1 DataFrame from _data/table_a1.parquet."""
    return pd.read_parquet(Path(data_dir) / "table_a1.parquet")


def format_table_a1_latex(table_a1):
    """Convert Table A1 DataFrame to a LaTeX tabular string.

    Parameters
    ----------
    table_a1 : pd.DataFrame
        Output of calc_summary_stats.calc_balance_sheet().

    Returns
    -------
    str
        LaTeX table source.
    """
    cols = list(table_a1.columns)
    col_spec = "l" + "r" * len(cols)

    latex_lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{Balance Sheet Composition of U.S.\ Commercial Banks as of {REPORT_DATE}. "
        r"Asset and liability categories are in \$billions. "
        r"Figures are derived from WRDS Call Report data. "
        r"Small banks have total assets $\leq$ \$1.384B.}",
        r"\label{tab:table_a1}",
        r"\small",
        rf"\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
        " & ".join(["Category"] + cols) + r" \\",
        r"\midrule",
    ]

    for item, row in table_a1.iterrows():
        values = [f"{v:.1f}" for v in row]
        item_escaped = item.replace("$", r"\$").replace("%", r"\%")
        line = f"{item_escaped} & " + " & ".join(values) + r" \\"
        latex_lines.append(line)

    latex_lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]

    return "\n".join(latex_lines)


def create_table_a1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    """Load Table A1 data, format as LaTeX, and save to _output/table_a1.tex."""
    table_a1 = load_table_a1(data_dir)
    latex_str = format_table_a1_latex(table_a1)

    output_path = Path(output_dir) / "table_a1.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    create_table_a1()
