"""Run the full MTM loss analysis and save intermediate results to _data/.

Orchestrates the complete computation pipeline:
  1. Load raw WRDS data and ETF prices from parquet cache
  2. Clean and extract balance sheet items for the report date
  3. Classify banks by size category
  4. Compute ETF-based price changes and RMBS multiplier
  5. Calculate per-bank MTM losses
  6. Compute deposit ratios
  7. Assemble Table 1, Table A1, and Figure A1 data
  8. Save all outputs as parquet in DATA_DIR

This script assumes pull_wrds.py and pull_etf_data.py have already been run
to cache the raw data in DATA_DIR.

Usage
-----
    python run_analysis.py
"""

import pandas as pd
from pathlib import Path

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
REPORT_DATE = config("REPORT_DATE")
MTM_END_DATE = config("MTM_END_DATE")

import pull_wrds
import pull_etf_data
import clean_data
import calc_mtm_losses
import calc_table1
import calc_summary_stats


def main():
    """Run full analysis pipeline and save outputs to DATA_DIR."""
    print(f"Report date: {REPORT_DATE}, MTM end date: {MTM_END_DATE}")

    # 1. Load raw data
    print("Loading WRDS data...")
    rcon1 = pull_wrds.load_rcon_series_1()
    rcon2 = pull_wrds.load_rcon_series_2()
    rcfd1 = pull_wrds.load_rcfd_series_1()
    rcfd2 = pull_wrds.load_rcfd_series_2()

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
    total_assets = calc_mtm_losses.classify_banks(total_assets)
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
    table_a1 = calc_summary_stats.calc_balance_sheet(
        rmbs, loans, treasuries, other_loans, total_assets, uninsured, insured
    )
    figure_a1_data = calc_summary_stats.calc_figure_a1_data(
        rmbs, loans, treasuries, other_loans, total_assets, bank_losses, uninsured, insured
    )

    # 9. Save all outputs
    bank_losses.to_parquet(DATA_DIR / "bank_losses.parquet")
    uninsured_ratio.to_parquet(DATA_DIR / "uninsured_ratio.parquet")
    insured_coverage.to_parquet(DATA_DIR / "insured_coverage.parquet")
    table1.to_parquet(DATA_DIR / "table1.parquet")
    table_a1.to_parquet(DATA_DIR / "table_a1.parquet")

    # Save figure data as a combined DataFrame (stacked)
    fig_df = pd.concat([
        figure_a1_data["assets_book"].rename("value").to_frame().assign(category="assets_book"),
        figure_a1_data["assets_mtm"].rename("value").to_frame().assign(category="assets_mtm"),
        figure_a1_data["liabilities"].rename("value").to_frame().assign(category="liabilities"),
    ])
    fig_df.index.name = "item"
    fig_df_pivot = fig_df.reset_index().pivot(index="category", columns="item", values="value")
    fig_df_pivot.to_parquet(DATA_DIR / "figure_a1_data.parquet")

    print("\nAll outputs saved to DATA_DIR.")


if __name__ == "__main__":
    main()
