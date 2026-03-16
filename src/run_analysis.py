"""Run the full MTM loss analysis and save intermediate results to _data/.

Orchestrates the complete computation pipeline:
  1. Load raw WRDS/FFIEC data and ETF prices from parquet cache
  2. Clean and extract balance sheet items for the report date
  3. Classify banks by size category
  4. Compute ETF-based price changes and RMBS multiplier
  5. Calculate per-bank MTM losses
  6. Compute deposit ratios
  7. Assemble Table 1, Table A1, and Figure A1 data
  8. Save all outputs as parquet in DATA_DIR

This script assumes pull_wrds.py (or pull_ffiec.py) and pull_etf_data.py
have already been run to cache the raw data in DATA_DIR.

Usage
-----
    python run_analysis.py                # WRDS (default)
    python run_analysis.py --source ffiec # FFIEC extension
"""

import argparse
import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

import pull_wrds
import pull_etf_data
import clean_data
import calc_mtm_losses
import calc_table1
import calc_summary_stats
from clean_data import (
    build_table_a1_raw_frames,
    build_table_a1_assets_from_raw,
    build_table_a1_liabilities_from_raw,
)

# ---------------------------------------------------------------------------
# Source-specific configuration
# ---------------------------------------------------------------------------

_SOURCE_CONFIG = {
    "wrds": {
        "report_date": config("REPORT_DATE"),
        "mtm_end_date": config("MTM_END_DATE"),
        "parquet_suffix": "",
        "struct_rel_file": "struct_rel_2022.parquet",
    },
    "ffiec": {
        "report_date": config("FFIEC_REPORT_DATE"),
        "mtm_end_date": config("FFIEC_MTM_END_DATE"),
        "parquet_suffix": "_ffiec",
        "struct_rel_file": "struct_rel_2024.parquet",
    },
}


def main(source="wrds"):
    """Run full analysis pipeline and save outputs to DATA_DIR."""

    cfg = _SOURCE_CONFIG[source]
    sfx = cfg["parquet_suffix"]
    REPORT_DATE = cfg["report_date"]
    MTM_END_DATE = cfg["mtm_end_date"]

    print(f"=== Running analysis: source={source} ===")
    print(f"Report date: {REPORT_DATE}, MTM end date: {MTM_END_DATE}")

    # 1. Load raw data
    print(f"Loading {source.upper()} data...")
    rcon1 = pd.read_parquet(DATA_DIR / f"RCON_Series_1{sfx}.parquet")
    rcon2 = pd.read_parquet(DATA_DIR / f"RCON_Series_2{sfx}.parquet")
    rcfd1 = pd.read_parquet(DATA_DIR / f"RCFD_Series_1{sfx}.parquet")
    rcfd2 = pd.read_parquet(DATA_DIR / f"RCFD_Series_2{sfx}.parquet")
    rcfn1 = pd.read_parquet(DATA_DIR / f"RCFN_Series_1{sfx}.parquet")

    print("Loading ETF data...")
    etf_raw = pull_etf_data.load_etf_data()

    # 2. Clean and extract balance sheet items
    print("Cleaning data...")
    rmbs = clean_data.get_rmbs(rcfd1, rcon1, rcon2, REPORT_DATE)
    treasuries = clean_data.get_treasuries(rcfd2, rcon2, REPORT_DATE)
    loans = clean_data.get_loans(rcon1, REPORT_DATE)
    other_loans = clean_data.get_other_loans(rcon2, rcfd1, REPORT_DATE)
    total_assets = clean_data.get_total_assets(rcfd2, rcon2, REPORT_DATE)
    uninsured = clean_data.get_uninsured_deposits(rcon1, REPORT_DATE)
    insured = clean_data.get_insured_deposits(rcon1, REPORT_DATE)

    # 3. Classify banks
    struct_rel = pd.read_parquet(DATA_DIR / cfg["struct_rel_file"])
    gsib_ids = calc_mtm_losses.build_gsib_ids(struct_rel)
    total_assets = calc_mtm_losses.classify_banks(total_assets, gsib_ids)
    print(f"Banks: {len(total_assets):,} total, "
          f"{(total_assets['size_category']=='GSIB').sum()} GSIB, "
          f"{(total_assets['size_category']=='Large non-GSIB').sum()} Large non-GSIB, "
          f"{(total_assets['size_category']=='Small').sum()} Small")

    # 4. ETF price changes
    etf_quarterly = clean_data.clean_etf_prices(etf_raw, REPORT_DATE, MTM_END_DATE)
    price_changes = calc_mtm_losses.calc_price_changes(etf_quarterly, REPORT_DATE, MTM_END_DATE)
    rmbs_multiplier = calc_mtm_losses.calc_rmbs_multiplier(etf_quarterly, REPORT_DATE, MTM_END_DATE)
    print(f"RMBS multiplier: {rmbs_multiplier:.4f}")
    print("Price changes by bucket:")
    for bucket, chg in price_changes.items():
        print(f"  {bucket}: {chg*100:.2f}%")

    # 5. Per-bank MTM losses
    print("Computing MTM losses...")
    bank_losses = calc_mtm_losses.calc_bank_losses(
        rmbs, loans, treasuries, other_loans, total_assets, price_changes, rmbs_multiplier
    )
    print(f"Aggregate loss: ${-bank_losses['total_loss'].sum()/1e9:.1f}B")

    # 6. Deposit ratios
    uninsured_ratio = calc_mtm_losses.calc_uninsured_deposit_ratio(uninsured, bank_losses)
    insured_coverage = calc_mtm_losses.calc_insured_deposit_coverage(insured, uninsured, bank_losses)

    # 7. Table 1
    table1 = calc_table1.calc_table1(bank_losses, uninsured_ratio, insured_coverage)
    print("\nTable 1:")
    print(table1.to_string())

    # 8. Table A1 and Figure A1 data
    # Build raw frames for Table A1 (notebook replication path)

    rcon_df, rcfd_df, rcfn_df= clean_data.build_table_a1_raw_frames(
    rcon1,
    rcon2,
    rcfd1,
    rcfd2,
    rcfn1,
    report_date=REPORT_DATE,
)

    bank_asset_a1 = clean_data.build_table_a1_assets_from_raw(
    rcon_df,
    rcfd_df,
    rcfn_df,
    total_assets
)
    bank_liab_a1 = clean_data.build_table_a1_liabilities_from_raw(
        rcon_df,
        rcfd_df,
        rcfn_df,
        total_assets
    )
    
    table_a1_panel_a = calc_summary_stats.calc_table_a1(bank_asset_a1)
    table_a1_panel_b = calc_summary_stats.calc_table_a1_panel_b(bank_liab_a1)
    figure_a1_data = calc_summary_stats.calc_figure_a1_data(
        rmbs, loans, treasuries, other_loans, total_assets, bank_losses, uninsured, insured
    )

    # 9. Save all outputs
    bank_losses.to_parquet(DATA_DIR / f"bank_losses{sfx}.parquet")
    uninsured_ratio.to_parquet(DATA_DIR / f"uninsured_ratio{sfx}.parquet")
    insured_coverage.to_parquet(DATA_DIR / f"insured_coverage{sfx}.parquet")
    table1.to_parquet(DATA_DIR / f"table1{sfx}.parquet")
    table_a1_panel_a.to_parquet(DATA_DIR / f"table_a1_panel_a{sfx}.parquet")
    table_a1_panel_b.to_parquet(DATA_DIR / f"table_a1_panel_b{sfx}.parquet")

    # Save figure data as a combined DataFrame (stacked)
    fig_df = pd.concat([
        figure_a1_data["assets_book"].rename("value").to_frame().assign(category="assets_book"),
        figure_a1_data["assets_mtm"].rename("value").to_frame().assign(category="assets_mtm"),
        figure_a1_data["liabilities"].rename("value").to_frame().assign(category="liabilities"),
    ])
    fig_df.index.name = "item"
    fig_df_pivot = fig_df.reset_index().pivot(index="category", columns="item", values="value")
    fig_df_pivot.to_parquet(DATA_DIR / f"figure_a1_data{sfx}.parquet")

    print("\nAll outputs saved to DATA_DIR.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MTM loss analysis pipeline.")
    parser.add_argument(
        "--source",
        choices=["wrds", "ffiec"],
        default="wrds",
        help="Data source: 'wrds' (default) or 'ffiec'",
    )
    args = parser.parse_args()
    main(source=args.source)