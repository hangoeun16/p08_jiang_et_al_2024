"""Generate Table 1 as a LaTeX file and save to _output/.
 
Loads pre-computed Table 1 data from _data/table1.parquet, formats it,
and writes a standalone LaTeX table to _output/table1.tex. This .tex file
is included via \\input{} in the main LaTeX document.
 
Usage
-----
    python create_table1.py                # WRDS (default)
    python create_table1.py --source ffiec # FFIEC extension
"""
 
import argparse
import pandas as pd
from pathlib import Path
 
from settings import config
 
DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
 
REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")
 
 
# Rows whose values need T/B/M unit formatting (raw values are in $billions)
_DOLLAR_ROWS = {"Aggregate Loss", "Bank Level Loss", "Bank Level Loss Std"}
 
 
# ---------------------------------------------------------------------------
# Unit formatting
# ---------------------------------------------------------------------------
 
 
def _format_dollar_value(val: float) -> str:
    """Format a dollar value with adaptive unit suffix.
 
    Selects T (trillions), B (billions), or M (millions) based on the
    magnitude of the value. Input is assumed to be in $billions.
 
    Parameters
    ----------
    val : float
        Dollar amount in billions.
 
    Returns
    -------
    str
        Formatted string, e.g. '2.2T', '146.0B', '22.3M'.
    """
    if pd.isna(val):
        return ""
    abs_val = abs(val)
    if abs_val >= 1000:
        return f"{val / 1000:.1f}T"
    elif abs_val >= 1:
        return f"{val:.1f}B"
    else:
        return f"{val * 1000:.1f}M"
 
 
# ---------------------------------------------------------------------------
# Load / format / save
# ---------------------------------------------------------------------------
 
def load_table1(data_dir=DATA_DIR, source="wrds"):
    """"Load Table 1 DataFrame from _data/table1.parquet.
 
    Parameters
    ----------
    data_dir : Path
        Directory containing table1.parquet.
    source : str
        'wrds' or 'ffiec'.
 
    Returns
    -------
    pd.DataFrame
        Table 1 with rows = statistics, columns = bank groups.
    """
    sfx = "_ffiec" if source == "ffiec" else ""
    return pd.read_parquet(Path(data_dir) / f"table1{sfx}.parquet")
 
 
def format_table1_latex(table1, report_date=REPORT_DATE, mtm_end_date=MTM_END_DATE):
    """Convert Table 1 DataFrame to a LaTeX tabular string.
 
    Applies paper-consistent unit formatting to dollar-denominated rows
    (Aggregate Loss, Bank Level Loss) and renders all other rows as
    plain rounded numbers.
 
    Parameters
    ----------
    table1 : pd.DataFrame
        Output of calc_table1.calc_table1(). Rows = statistics,
        columns = bank groups (All Banks, Small, Large non-GSIB, GSIB).
 
    Returns
    -------
    str
        LaTeX table source ready for \\input{} inclusion.
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
        if stat in _DOLLAR_ROWS:
            values = [_format_dollar_value(v) for v in row]
        elif stat == "Number of Banks":
            values = [f"{int(v):,}" for v in row]
        else:
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
 
def create_table1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR, source="wrds"):
    """Load Table 1 data, format as LaTeX, and save to _output/table1[_ffiec].tex.
 
    Parameters
    ----------
    data_dir : Path
        Directory containing table1.parquet.
    output_dir : Path
        Directory where table1.tex will be written.
    source : str
        'wrds' or 'ffiec'.
    """
    sfx = "_ffiec" if source == "ffiec" else ""
    report_date = config("FFIEC_REPORT_DATE") if source == "ffiec" else REPORT_DATE
    mtm_end_date = config("FFIEC_MTM_END_DATE") if source == "ffiec" else MTM_END_DATE
 
    table1 = load_table1(data_dir, source)
    latex_str = format_table1_latex(table1, report_date, mtm_end_date)
 
    output_path = Path(output_dir) / f"table1{sfx}.tex"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(latex_str)
    print(f"Saved: {output_path}")
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Table 1 LaTeX file.")
    parser.add_argument("--source", choices=["wrds", "ffiec"], default="wrds")
    args = parser.parse_args()
    create_table1(source=args.source)