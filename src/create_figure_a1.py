"""Generate Figure A1: Aggregate Bank Assets and Liabilities bar chart.

Loads pre-computed summary data from _data/figure_a1_data.parquet and
produces a stacked bar chart comparing:
  - Book-value asset breakdown
  - Mark-to-market asset breakdown
  - Liability and equity breakdown

Saves the figure to _output/figure_a1.pdf and _output/figure_a1.png.

Usage
-----
    python create_figure_a1.py
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")


def load_figure_a1_data(data_dir=DATA_DIR):
    """Load Figure A1 input data from _data/figure_a1_data.parquet."""
    return pd.read_parquet(Path(data_dir) / "figure_a1_data.parquet")


def create_figure_a1(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    """Create and save Figure A1 as PDF and PNG.

    Produces a grouped stacked bar chart with three bars:
      1. Book-value assets (Q1 2022)
      2. MTM assets (Q1 2023 prices)
      3. Liabilities & equity (Q1 2022)

    Parameters
    ----------
    data_dir : Path
    output_dir : Path
    """
    data = load_figure_a1_data(data_dir)
    assets_book = data.loc["assets_book"]
    assets_mtm = data.loc["assets_mtm"]
    liabilities = data.loc["liabilities"]

    # Color palette
    colors_assets = {
        "RMBS": "#2196F3",
        "First-Lien Mortgages": "#03A9F4",
        "Treasury/Other Securities": "#4CAF50",
        "Other Loans": "#FF9800",
        "Other Assets": "#9E9E9E",
    }
    colors_liab = {
        "Insured Deposits": "#C8E6C9",
        "Uninsured Deposits": "#EF9A9A",
        "Other Liabilities & Equity": "#B0BEC5",
    }

    fig, ax = plt.subplots(figsize=(10, 6))

    bar_width = 0.25
    positions = [0, 0.3, 0.65]
    labels = [
        f"Assets (Book)\n{REPORT_DATE}",
        f"Assets (MTM)\n{MTM_END_DATE}",
        f"Liabilities\n{REPORT_DATE}",
    ]

    def _plot_stacked(ax, pos, series, colors, width=bar_width):
        bottom = 0
        for category, value in series.items():
            if value > 0:
                ax.bar(pos, value, width=width, bottom=bottom,
                       color=colors.get(category, "#607D8B"), label=category)
                bottom += value

    # Book-value assets
    _plot_stacked(ax, positions[0], assets_book, colors_assets)
    # MTM assets
    _plot_stacked(ax, positions[1], assets_mtm, colors_assets)
    # Liabilities
    _plot_stacked(ax, positions[2], liabilities, colors_liab)

    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel("$ Trillions", fontsize=11)
    ax.set_title(
        "Figure A1: Aggregate U.S. Commercial Bank Balance Sheet\n"
        f"(Book Value {REPORT_DATE} vs. MTM {MTM_END_DATE})",
        fontsize=12,
    )

    # Build legend (de-duplicate)
    handles, seen = [], set()
    for label, color in {**colors_assets, **colors_liab}.items():
        if label not in seen:
            seen.add(label)
            handles.append(mpatches.Patch(color=color, label=label))
    ax.legend(handles=handles, loc="upper right", fontsize=8, ncol=2)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / "figure_a1.pdf", dpi=150, bbox_inches="tight")
    fig.savefig(output_dir / "figure_a1.png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {output_dir / 'figure_a1.pdf'}")
    print(f"Saved: {output_dir / 'figure_a1.png'}")


if __name__ == "__main__":
    create_figure_a1()
