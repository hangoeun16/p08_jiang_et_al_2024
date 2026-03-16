"""Generate a bank fragility scatter plot: MTM loss/assets vs. uninsured deposit ratio.

Loads bank_losses.parquet and uninsured_ratio.parquet from _data/ and produces
a scatter plot showing the joint distribution of each bank's mark-to-market
loss rate (x-axis) and uninsured deposit exposure (y-axis), colored by size
category.  Saves to _output/figure_fragility.pdf and _output/figure_fragility.png.

Usage
-----
    python create_fragility_figure.py
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))
REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")

# Plot style per size category: (fill color, marker size, alpha)
_STYLE = {
    "Small":          ("#aec7e8", 6,  0.20),
    "Large non-GSIB": ("#1f77b4", 25, 0.65),
    "GSIB":           ("#d62728", 70, 1.00),
}
_ORDER = ["Small", "Large non-GSIB", "GSIB"]


def load_fragility_data(data_dir=DATA_DIR):
    """Merge bank losses and uninsured ratios into a single scatter DataFrame.

    Parameters
    ----------
    data_dir : Path

    Returns
    -------
    pd.DataFrame
        Columns: bank_id, size_category, x (loss/assets, %), y (uninsured/MTM assets, %)
    """
    bank_losses = pd.read_parquet(Path(data_dir) / "bank_losses.parquet")
    uninsured = pd.read_parquet(Path(data_dir) / "uninsured_ratio.parquet")

    df = bank_losses[["bank_id", "size_category", "loss_over_assets"]].merge(
        uninsured[["bank_id", "uninsured_over_mtm_assets"]],
        on="bank_id",
        how="inner",
    )
    df = df.dropna(subset=["loss_over_assets", "uninsured_over_mtm_assets"])
    df["x"] = df["loss_over_assets"].abs() * 100
    df["y"] = df["uninsured_over_mtm_assets"] * 100
    return df


def create_fragility_figure(data_dir=DATA_DIR, output_dir=OUTPUT_DIR):
    """Create the fragility scatter plot and save to _output/.

    Parameters
    ----------
    data_dir : Path
    output_dir : Path
    """
    df = load_fragility_data(data_dir)

    # Clip extreme outliers for readability while preserving the bulk of the distribution
    df = df[(df["x"] <= 25) & (df["y"] <= 100)]

    fig, ax = plt.subplots(figsize=(8, 6))

    # Light background shading for the high-risk quadrant
    ax.axhspan(50, 100, alpha=0.04, color="#d62728", zorder=0)
    ax.axvspan(9, 25,  alpha=0.04, color="#d62728", zorder=0)

    # Plot Small first so Large/GSIB render on top and remain visible
    for cat in _ORDER:
        sub = df[df["size_category"] == cat]
        color, size, alpha = _STYLE[cat]
        ax.scatter(
            sub["x"], sub["y"],
            c=color, s=size, alpha=alpha,
            linewidths=0,
            label=f"{cat} (n={len(sub):,})",
        )

    # Reference lines
    ax.axhline(50, color="#d62728", linestyle="--", linewidth=0.9, alpha=0.5,
               label="50% uninsured threshold")
    ax.axvline(9,  color="#888888", linestyle=":",  linewidth=0.8, alpha=0.5)

    ax.set_xlabel("MTM Loss / Total Assets (%)", fontsize=11)
    ax.set_ylabel("Uninsured Deposits / MTM Assets (%)", fontsize=11)
    ax.set_xlim(0, 25)
    ax.set_ylim(0, 100)
    ax.legend(loc="upper right", fontsize=9, framealpha=0.9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_dir / "figure_fragility.pdf", bbox_inches="tight")
    plt.savefig(output_dir / "figure_fragility.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_dir / 'figure_fragility.pdf'}")
    print(f"Saved: {output_dir / 'figure_fragility.png'}")


if __name__ == "__main__":
    create_fragility_figure()
