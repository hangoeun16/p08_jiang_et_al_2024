"""Generate Table A1 (balance sheet composition) as a LaTeX file.

Loads pre-computed balance sheet data from _data/table_a1.parquet and
writes a LaTeX table to _output/table_a1.tex for inclusion in the report.

Usage
-----
    python create_table_a1.py                # WRDS (default)
    python create_table_a1.py --source ffiec # FFIEC extension
"""

import argparse
import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")


def load_table_a1_panels(data_dir=DATA_DIR, source="wrds"):
    sfx = "_ffiec" if source == "ffiec" else ""
    panel_a = pd.read_parquet(Path(data_dir) / f"table_a1_panel_a{sfx}.parquet")
    panel_b = pd.read_parquet(Path(data_dir) / f"table_a1_panel_b{sfx}.parquet")
    return panel_a, panel_b

def _fmt(v) -> str:
    """Format a value as a string with one decimal place."""
    if pd.isna(v):
        return ""
    return f"{v:.1f}"


def _fmt_dollars(v) -> str:
    """Format a dollar value (in $thousands) with T/B/M suffix."""
    if pd.isna(v):
        return ""
    v_abs = abs(v)
    if v_abs >= 1_000_000_000:  # trillions
        return f"{v / 1_000_000_000:.1f}T"
    elif v_abs >= 1_000_000:  # billions
        return f"{v / 1_000_000:.1f}B"
    elif v_abs >= 1_000:  # millions
        return f"{v / 1_000:.1f}M"
    else:
        return f"{v:.1f}K"


def _fmt_int(v) -> str:
    """Format a value as an integer (no decimals)."""
    if pd.isna(v):
        return ""
    return f"{int(v):,}"


def _format_panel_rows(df: pd.DataFrame) -> list[str]:
    """Convert a panel DataFrame into LaTeX row strings."""
    lines = []

    for item, row in df.iterrows():
        item_str = str(item)
        item_escaped = item_str.replace("&", r"\&").replace("$", r"\$").replace("%", r"\%")

        # Pick formatter based on row label
        if "Total Asset" in item_str:
            fmt = _fmt_dollars
        elif "Number of Banks" in item_str:
            fmt = _fmt_int
        else:
            fmt = _fmt

        mean_vals = [
            fmt(row.get("Aggregate")),
            fmt(row.get("Full sample(mean)")),
            fmt(row.get("small(mean)")),
            fmt(row.get("large(mean)")),
            fmt(row.get("GSIB(mean)")),
        ]

        sd_vals = [
            "",
            fmt(row.get("Full sample(sd)")),
            fmt(row.get("small(sd)")),
            fmt(row.get("large(sd)")),
            fmt(row.get("GSIB(sd)")),
        ]

        lines.append(f"{item_escaped} & " + " & ".join(mean_vals) + r" \\")
        # Skip the sd row for "Number of Banks" (all empty anyway)
        if "Number of Banks" not in item_str:
            lines.append(" & " + " & ".join(sd_vals) + r" \\")
    return lines

def _table_header():
    """Return the shared column header line."""
    return r"Category & Aggregate & Full sample & Small & Large non-GSIB & GSIB \\"


def _caption_text(report_date=REPORT_DATE):
    """Return the shared caption text fragment."""
    return (
        rf"Balance Sheet Composition of U.S.\ Commercial Banks as of {report_date}. "
        r"Entries are percentages of total assets. "
        r"All numbers except for aggregate are based on sample averages "
        r"after winsorizing at the 5th and 95th percentiles. "
        r"Standard deviations are shown on the line below each mean. "
        r"Small banks have total assets $\leq$ \$1.384B."
    )


def format_table_a1_latex(panel_a, panel_b, report_date=REPORT_DATE, label_suffix=""):
    """Generate LaTeX for Table A1 as two separate tables (Panel A and B)."""
    col_spec = "lrrrrr"

    # Convert report_date to quarter label (e.g. "2022-03-31" → "Q1 2022")
    rd = pd.Timestamp(report_date)
    quarter_label = f"Q{rd.quarter} {rd.year}"

    # --- Panel A ---
    lines_a = [
        r"\begin{table}[htbp]",
        r"\centering",
        rf"\caption{{{_caption_text(report_date)}}}",
        rf"\label{{tab:table_a1{label_suffix}}}",
        r"\footnotesize",
        rf"\resizebox{{\textwidth}}{{!}}{{",
        rf"\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
        _table_header(),
        r"\midrule",
        rf"\multicolumn{{6}}{{c}}{{\textbf{{Panel A: Bank Asset Composition, {quarter_label}}}}} \\",
        r"\midrule",
    ]
    lines_a += _format_panel_rows(panel_a)
    lines_a += [
        r"\bottomrule",
        r"\end{tabular}",
        r"}",  # close \resizebox
        r"\end{table}",
    ]

    # --- Panel B ---
    lines_b = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Balance Sheet Composition (continued).}",
        rf"\label{{tab:table_a1_b{label_suffix}}}",
        r"\footnotesize",
        rf"\resizebox{{\textwidth}}{{!}}{{",
        rf"\begin{{tabular}}{{{col_spec}}}",
        r"\toprule",
        _table_header(),
        r"\midrule",
        rf"\multicolumn{{6}}{{c}}{{\textbf{{Panel B: Bank Liability Composition, {quarter_label}}}}} \\",
        r"\midrule",
    ]
    lines_b += _format_panel_rows(panel_b)
    lines_b += [
        r"\bottomrule",
        r"\end{tabular}",
        r"}",  # close \resizebox
        r"\end{table}",
    ]

    return "\n".join(lines_a) + "\n\n\\clearpage\n\n" + "\n".join(lines_b)


def create_table_a1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR, source="wrds"):
    sfx = "_ffiec" if source == "ffiec" else ""
    report_date = config("FFIEC_REPORT_DATE") if source == "ffiec" else REPORT_DATE
    panel_a, panel_b = load_table_a1_panels(data_dir, source)
    latex_str = format_table_a1_latex(panel_a, panel_b, report_date, label_suffix=sfx)

    output_path = Path(output_dir) / f"table_a1{sfx}.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Table A1 LaTeX file.")
    parser.add_argument("--source", choices=["wrds", "ffiec"], default="wrds")
    args = parser.parse_args()
    create_table_a1(source=args.source)