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


def load_table_a1_panels(data_dir=DATA_DIR):
    panel_a = pd.read_parquet(Path(data_dir) / "table_a1_panel_a.parquet")
    panel_b = pd.read_parquet(Path(data_dir) / "table_a1_panel_b.parquet")
    return panel_a, panel_b

def _fmt(v) -> str:
    if pd.isna(v):
        return ""
    return f"{v:.1f}"

def _format_panel_rows(df: pd.DataFrame) -> list[str]:
    lines = []

    for item, row in df.iterrows():
        item_escaped = str(item).replace("&", r"\&").replace("$", r"\$").replace("%", r"\%")

        mean_vals = [
            _fmt(row.get("Aggregate")),
            _fmt(row.get("Full sample(mean)")),
            _fmt(row.get("small(mean)")),
            _fmt(row.get("large(mean)")),
            _fmt(row.get("GSIB(mean)")),
        ]

        sd_vals = [
            "",
            _fmt(row.get("Full sample(sd)")),
            _fmt(row.get("small(sd)")),
            _fmt(row.get("large(sd)")),
            _fmt(row.get("GSIB(sd)")),
        ]

        lines.append(f"{item_escaped} & " + " & ".join(mean_vals) + r" \\")
        lines.append(" & " + " & ".join(sd_vals) + r" \\")
    return lines

def format_table_a1_latex(panel_a, panel_b):
    cols = list(panel_a.columns)
    col_spec = "lrrrrr"

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{Balance Sheet Composition of U.S.\ Commercial Banks as of {REPORT_DATE}. "
        r"Entries are percentages of total assets. "
        r"All numbers except for aggregate are based on sample averages after winsorizing at the 5th and 95th percentiles. "
        r"Standard deviations are shown on the line below each mean. "
        r"Small banks have total assets $\leq$ \$1.384B.}",
        r"\label{tab:table_a1}",
        r"\small",
        rf"\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
        r"Category & Aggregate & Full sample & Small & Large non-GSIB & GSIB \\",
        r"\midrule",
        r"\multicolumn{6}{c}{\textbf{Panel A: Bank Asset Composition, Q1 2022}} \\",
        r"\midrule",
    ]


    lines += _format_panel_rows(panel_a)

    lines += [
        r"\midrule",
        r"\multicolumn{6}{c}{\textbf{Panel B: Bank Liability Composition, Q1 2022}} \\",
        r"\midrule",
    ]

    lines += _format_panel_rows(panel_b)

    lines += [
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ]
    return "\n".join(lines)


def create_table_a1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    panel_a, panel_b = load_table_a1_panels(data_dir)
    latex_str = format_table_a1_latex(panel_a, panel_b)

    output_path = Path(output_dir) / "table_a1.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")



if __name__ == "__main__":
    create_table_a1()
