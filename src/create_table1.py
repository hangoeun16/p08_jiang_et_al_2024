"""Generate Table 1 as a LaTeX file and save to _output/.

Loads pre-computed Table 1 data from _data/table1.parquet, formats it,
and writes a standalone LaTeX table to _output/table1.tex. This .tex file
is included via \\input{} in the main LaTeX document.

Usage
-----
    python create_table1.py
"""

import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")


def load_table1(data_dir=DATA_DIR):
    """Load Table 1 DataFrame from _data/table1.parquet."""
    return pd.read_parquet(Path(data_dir) / "table1.parquet")


def format_table1_latex(table1):
    """Convert Table 1 DataFrame to a LaTeX tabular string.

    Parameters
    ----------
    table1 : pd.DataFrame
        Output of calc_table1.calc_table1().

    Returns
    -------
    str
        LaTeX table source.
    """
    latex_lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{MTM Losses from Monetary Tightening, {REPORT_DATE} to {MTM_END_DATE}. "
        r"Loss calculations follow Jiang et al.\ (2024). "
        r"Aggregate Loss is in \$billions. "
        r"Small banks have total assets $\leq$ \$1.384B; GSIBs are global systemically important banks.}",
        r"\label{tab:table1}",
        r"\small",
        r"\begin{tabular}{lrrrr}",
        r"\toprule",
        " & ".join(["", "All Banks", "Small", "Large non-GSIB", "GSIB"]) + r" \\",
        r"\midrule",
    ]

    for stat, row in table1.iterrows():
        values = [str(v) for v in row]
        stat_escaped = stat.replace("$", r"\$").replace("%", r"\%")
        line = f"{stat_escaped} & " + " & ".join(values) + r" \\"
        latex_lines.append(line)

    latex_lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]

    return "\n".join(latex_lines)


def create_table1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    """Load Table 1 data, format as LaTeX, and save to _output/table1.tex.

    Parameters
    ----------
    data_dir : Path
    output_dir : Path
    """
    table1 = load_table1(data_dir)
    latex_str = format_table1_latex(table1)

    output_path = Path(output_dir) / "table1.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    create_table1()
