"""Generate Figure A1: Aggregate Asset and Liabilities of U.S. Banks.

Replicates the two-horizontal-bar layout from Figure A1 of
Jiang et al. (2024).  Top bar = asset composition, bottom bar =
liability + equity composition, both in $ trillions.

Reads aggregate percentages from the Table A1 panel parquets and
converts them to dollar amounts using total assets.

Saves the figure to _output/figure_a1.pdf and _output/figure_a1.png.

Usage
-----
    python create_figure_a1.py                # WRDS (default)
    python create_figure_a1.py --source ffiec # FFIEC extension
"""

import argparse
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")


def load_figure_a1_data(data_dir=DATA_DIR, source="wrds"):
    """Load Table A1 panel data and extract aggregate figures for the chart.

    Returns
    -------
    tuple of (dict, dict, float)
        asset_items : dict mapping category name to $ trillions
        liability_items : dict mapping category name to $ trillions
        total_assets_t : total assets in $ trillions
    """
    sfx = "_ffiec" if source == "ffiec" else ""
    panel_a = pd.read_parquet(Path(data_dir) / f"table_a1_panel_a{sfx}.parquet")
    panel_b = pd.read_parquet(Path(data_dir) / f"table_a1_panel_b{sfx}.parquet")

    agg_a = panel_a["Aggregate"]
    agg_b = panel_b["Aggregate"]

    # Total assets in $thousands → $trillions
    total_assets_t = agg_a["Total Asset $"] / 1e9

    # Asset composition (aggregate % → $ trillions)
    cash_pct = agg_a["Cash"]
    security_pct = agg_a["Security"]
    re_loan_pct = agg_a["Real Estate Loan"]
    other_loan_pct = agg_a["Total Loan"] - re_loan_pct
    other_asset_pct = 100 - cash_pct - security_pct - agg_a["Total Loan"]

    asset_items = {
        "Cash": cash_pct / 100 * total_assets_t,
        "Security": security_pct / 100 * total_assets_t,
        "Real Estate\nLoan": re_loan_pct / 100 * total_assets_t,
        "Other\nLoan": other_loan_pct / 100 * total_assets_t,
        "Other\nAsset": other_asset_pct / 100 * total_assets_t,
    }

    # Liability + equity composition (aggregate % → $ trillions)
    insured_pct = agg_b["Insured Deposit"]
    uninsured_pct = agg_b["Uninsured Deposit"]
    equity_pct = agg_b["Total Equity"]
    other_pct = 100 - insured_pct - uninsured_pct - equity_pct

    liability_items = {
        "Insured\nDeposits": insured_pct / 100 * total_assets_t,
        "Uninsured\nDeposits": uninsured_pct / 100 * total_assets_t,
        "Other": other_pct / 100 * total_assets_t,
        "Total\nEquity": equity_pct / 100 * total_assets_t,
    }

    return asset_items, liability_items, total_assets_t


def create_figure_a1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR, source="wrds"):
    """Create and save Figure A1 as PDF and PNG.

    Produces two horizontal stacked bar charts:
      1. Total Assets (Trillion)  — Cash, Security, RE Loan, Other Loan, Other
      2. Total Liability (Trillion) — Insured, Uninsured, Other, Equity

    Parameters
    ----------
    data_dir : Path
    output_dir : Path
    source : str
        'wrds' or 'ffiec'.
    """
    sfx = "_ffiec" if source == "ffiec" else ""
    asset_items, liability_items, total_assets_t = load_figure_a1_data(data_dir, source)

    # --- Colors matching the paper ---
    asset_colors = ["#4472C4", "#6FA0D6", "#A9C4E0", "#D6C6A0", "#E8B87D"]
    liab_colors = ["#943735", "#D6756E", "#C0C0C0", "#FFFFFF"]

    fig, axes = plt.subplots(2, 1, figsize=(10, 5.5), gridspec_kw={"hspace": 0.55})

    # Round up to nice x-axis max
    x_max = int(np.ceil(total_assets_t / 6) * 6)

    for ax_idx, (items, colors, title) in enumerate([
        (asset_items, asset_colors, "Total Assets (Trillion)"),
        (liability_items, liab_colors, "Total Liability (Trillion)"),
    ]):
        ax = axes[ax_idx]
        names = list(items.keys())
        values = list(items.values())

        left = 0
        bars = []
        for i, (name, val) in enumerate(zip(names, values)):
            edgecolor = "black" if ax_idx == 1 else "white"
            bar = ax.barh(
                0, val, left=left, height=0.5,
                color=colors[i], edgecolor=edgecolor, linewidth=0.8,
            )
            bars.append(bar)

            # Label inside each segment
            cx = left + val / 2
            # Use black text, except for dark backgrounds use white
            text_color = "white" if ax_idx == 1 and i < 2 else "black"
            if val / total_assets_t > 0.06:  # only label if segment is wide enough
                ax.text(
                    cx, 0, name, ha="center", va="center",
                    fontsize=9, fontweight="bold", color=text_color,
                )
            left += val

        # Equity bracket annotation (bottom bar only)
        if ax_idx == 1:
            equity_val = values[-1]
            equity_left = left - equity_val
            bracket_y = 0.38
            ax.annotate(
                "Total\nEquity",
                xy=(equity_left + equity_val / 2, bracket_y),
                xytext=(equity_left + equity_val / 2, bracket_y + 0.28),
                ha="center", va="bottom", fontsize=9, fontweight="bold",
                arrowprops=dict(arrowstyle="-[, widthB=1.5, lengthB=0.3",
                                lw=1.2, color="black"),
            )

        ax.set_xlim(0, x_max)
        ax.set_ylim(-0.5, 1.0)
        ax.set_xticks(range(0, x_max + 1, 6))
        ax.set_yticks([])
        ax.set_title(title, fontsize=12, fontweight="bold", loc="left", pad=10)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)

    plt.tight_layout()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / f"figure_a1{sfx}.pdf", dpi=150, bbox_inches="tight")
    fig.savefig(output_dir / f"figure_a1{sfx}.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {output_dir / f'figure_a1{sfx}.pdf'}")
    print(f"Saved: {output_dir / f'figure_a1{sfx}.png'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Figure A1.")
    parser.add_argument("--source", choices=["wrds", "ffiec"], default="wrds")
    args = parser.parse_args()
    create_figure_a1(source=args.source)