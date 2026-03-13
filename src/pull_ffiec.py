"""Pull and cache FFIEC Call Report data needed to extend Jiang et al. (2024).

Fetches four Call Report data series from FFIEC bulk Call Report schedules:
  - RCON_Series_1_ffiec: domestic balance sheet items (RMBS, first-lien mortgages,
    uninsured deposits, insured deposits) from pull_rcon_series_1
  - RCON_Series_2_ffiec: domestic securities (treasuries/other), other loans,
    total assets from pull_rcon_series_2
  - RCFD_Series_1_ffiec: domestic+foreign balance sheet items (RMBS, first-lien
    mortgages, other loans, cash) from pull_fcfd_series1
  - RCFD_Series_2_ffiec: domestic+foreign treasury securities, total assets
    from pull_rcfd_series_2

Raw FFIEC bulk files are downloaded from the FFIEC website and extracted
locally. The schedules are then parsed and merged into bank-quarter panels
using IDRSSD as the bank identifier.

Each pull function saves results as parquet to DATA_DIR. Corresponding
load_* functions read from those cached parquet files.

Usage
-----
Run directly to download FFIEC bulk files and cache all series:
    python pull_ffiec.py

Or import and use individual pull/load functions:
    from pull_ffiec import load_rcon_series_1
"""



import glob
import os
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from settings import config


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(config("DATA_DIR"))
FFIEC_RAW_DIR = Path(config("FFIEC_RAW_DIR", default="ffiec_data"))

# Download window for raw FFIEC bulk files
#START_YEAR = int(config("FFIEC_START_YEAR", default=2023))
#END_YEAR = int(config("FFIEC_END_YEAR", default=2025))

# Panel-construction window
FFIEC_START_DATE = config("FFIEC_START_DATE")
FFIEC_END_DATE = config("FFIEC_END_DATE")
START_YEAR = config("FFIEC_START_YEAR")
END_YEAR = config("FFIEC_END_YEAR")

#FFIEC_START_DATE = config("FFIEC_START_DATE", default="2023-12-31")
#FFIEC_END_DATE = config("FFIEC_END_DATE", default="2025-12-31")

FFIEC_BASE_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
DOWNLOAD_TIMEOUT = config("FFIEC_DOWNLOAD_TIMEOUT")



# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------


def _wait_for_download(data_dir: Path, timeout: int = DOWNLOAD_TIMEOUT) -> None:
    """Wait for Chrome to finish downloading files into `data_dir`.

    Checks for temporary `.crdownload` files and waits until all downloads
    complete or until the timeout is reached.

    Parameters
    ----------
    data_dir : Path
        Directory where Chrome downloads files.
    timeout : int
        Maximum number of seconds to wait for download completion.

    Returns
    -------
    None
    """
    start = time.time()
    while time.time() - start < timeout:
        partials = list(data_dir.glob("*.crdownload"))
        if not partials:
            return
        time.sleep(1)
    raise TimeoutError(f"Download did not finish within {timeout} seconds.")


def _unzip_all(data_dir: Path) -> None:
    """Extract all zip archives in `data_dir`.

    Unzips all `.zip` files found in the target directory and removes the
    archive files after successful extraction.

    Parameters
    ----------
    data_dir : Path
        Directory containing downloaded FFIEC zip archives.

    Returns
    -------
    None
    """
    zip_files = sorted(data_dir.glob("*.zip"))
    if not zip_files:
        print("No zip archives found to extract.")
        return

    import zipfile

    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(data_dir)
        print(f"  Extracted: {zip_path.name}")
        zip_path.unlink()

    print("All archives extracted.")


def pull_call_report_single_period(
    data_dir: Path = FFIEC_RAW_DIR,
    start_year: int = START_YEAR,
    end_year: int = END_YEAR,
) -> None:
    """Pull FFIEC bulk Call Report files from the FFIEC website.

    Downloads quarterly bulk Call Report schedule files for the requested
    year range from the FFIEC website and extracts them into the target
    directory.

    Parameters
    ----------
    data_dir : Path
        Directory where downloaded and extracted FFIEC files will be stored.
    start_year : int    
        First year to download.
    end_year : int
        Last year to download.

    Returns
    -------
    None
    """
    data_dir.mkdir(parents=True, exist_ok=True)

    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": str(data_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    try:
        driver = webdriver.Chrome(options=chrome_options)
    except WebDriverException as exc:
        raise RuntimeError(
            "Could not start Chrome WebDriver. Make sure Chrome/Chromedriver "
            "is installed and available."
        ) from exc

    try:
        driver.get(FFIEC_BASE_URL)
        time.sleep(2)

        for year in range(start_year, end_year + 1):
            for quarter_code in ["0331", "0630", "0930", "1231"]:
                period_text = f"{quarter_code}{year}"

                try:
                    period_dropdown = Select(driver.find_element(By.ID, "MainContent_ddlReportingPeriod"))
                    period_dropdown.select_by_visible_text(period_text)
                    time.sleep(1)

                    product_dropdown = Select(driver.find_element(By.ID, "MainContent_ddlProduct"))
                    product_dropdown.select_by_visible_text("Call Reports -- Single Period")
                    time.sleep(1)

                    file_dropdown = Select(driver.find_element(By.ID, "MainContent_ddlFileFormat"))
                    file_dropdown.select_by_visible_text("Bulk All Schedules")
                    time.sleep(1)

                    download_button = driver.find_element(By.ID, "MainContent_btnDownload")
                    download_button.click()

                    print(f"  Downloading FFIEC bulk file for {period_text} ...")
                    _wait_for_download(data_dir)
                    time.sleep(1)

                except Exception as exc:
                    print(f"  Skipping {period_text} due to error: {exc}")

    finally:
        driver.quit()

    _unzip_all(data_dir)


# ---------------------------------------------------------------------------
# Raw FFIEC readers
# ---------------------------------------------------------------------------


def _quarter_date_strings(start_date: str, end_date: str) -> list[str]:
    """Construct quarter-end date strings for the FFIEC panel window.

    Generates quarter-end dates between `start_date` and `end_date` and
    returns them in MMDDYYYY format for use in FFIEC file names.

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.

    Returns
    -------
    list[str]
    """
    quarter_ends = pd.date_range(start=start_date, end=end_date, freq="Q")
    return [dt.strftime("%m%d%Y") for dt in quarter_ends]


def _read_ffiec_file(path: Path) -> pd.DataFrame:
    """Read a raw FFIEC schedule file into a cleaned DataFrame.

    Parses a tab-delimited FFIEC schedule text file, removes trailing unnamed
    columns, drops the descriptive row beneath the header when present, and
    standardizes IDRSSD for downstream merging.

    Parameters
    ----------
    path : Path
        Path to a raw FFIEC schedule text file.

    Returns
    -------
    pd.DataFrame
    """
    df = pd.read_csv(path, sep="\t", low_memory=False)

    # Drop trailing unnamed columns
    df = df.loc[:, ~df.columns.str.contains(r"^Unnamed")]

    # Drop FFIEC description row if present
    if len(df) > 0 and pd.isna(df.iloc[0, 0]):
        df = df.iloc[1:].copy()

    # Standardize first column name to IDRSSD
    df = df.rename(columns={df.columns[0]: "IDRSSD"})

    # Clean IDRSSD
    df["IDRSSD"] = pd.to_numeric(df["IDRSSD"], errors="coerce")
    df = df.dropna(subset=["IDRSSD"]).copy()
    df["IDRSSD"] = df["IDRSSD"].astype(int).astype(str)

    # Convert everything else to numeric where possible
    for c in df.columns:
        if c != "IDRSSD":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.reset_index(drop=True)


def load_ent(date_str: str, data_dir: Path = FFIEC_RAW_DIR) -> pd.DataFrame:
    """Load the ENT schedule for a given FFIEC reporting quarter.

    Loads the ENT schedule corresponding to the requested quarter and returns
    the cleaned identifier backbone used to construct bank-quarter panels.

    Parameters
    ----------
    date_str : str
        Quarter-end date in MMDDYYYY format.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    path = data_dir / f"FFIEC CDR Call Schedule ENT {date_str}.txt"
    df = _read_ffiec_file(path)
    return df[["IDRSSD"]].copy()


def load_schedule(schedule: str, date_str: str, data_dir: Path = FFIEC_RAW_DIR) -> pd.DataFrame:
    """Load a raw FFIEC schedule for a given reporting quarter.

    Loads a single FFIEC schedule file for the requested quarter. If the
    schedule is split across multiple files, the parts are merged on IDRSSD.

    Parameters
    ----------
    schedule : str
        FFIEC schedule code, such as RC, RCB, RCCI, RCO, or RCA.
    date_str : str
        Quarter-end date in MMDDYYYY format.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    base = data_dir / f"FFIEC CDR Call Schedule {schedule} {date_str}.txt"
    if base.exists():
        return _read_ffiec_file(base)

    pattern = str(data_dir / f"FFIEC CDR Call Schedule {schedule} {date_str}*.txt")
    matches = sorted(glob.glob(pattern))

    if not matches:
        raise FileNotFoundError(f"No FFIEC file found for schedule={schedule}, date={date_str}")

    # If split files exist, merge them on IDRSSD
    dfs = [_read_ffiec_file(Path(fp)) for fp in matches]

    merged = dfs[0]
    for part in dfs[1:]:
        overlap = [c for c in part.columns if c in merged.columns and c != "IDRSSD"]
        if overlap:
            part = part.drop(columns=overlap)
        merged = merged.merge(part, on="IDRSSD", how="outer")

    return merged


# ---------------------------------------------------------------------------
# Quarter builder
# ---------------------------------------------------------------------------


def _build_single_quarter(
    date_str: str,
    schedules_and_cols: dict[str, list[str]],
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Build a single bank-quarter panel from FFIEC raw schedules.

    Loads the ENT identifier schedule for the requested quarter, pulls the
    requested FFIEC schedules, and merges them into a single bank-quarter
    panel using IDRSSD as the bank identifier.

    Parameters
    ----------
    date_str : str
        Quarter-end date in MMDDYYYY format.
    schedules_and_cols : dict[str, list[str]]
        Mapping from FFIEC schedule code to the list of variables to extract
        from that schedule.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    ent = load_ent(date_str, data_dir)
    result = ent.copy()

    for sched_name, cols in schedules_and_cols.items():
        try:
            sched_df = load_schedule(sched_name, date_str, data_dir)
        except FileNotFoundError:
            continue

        available = [c for c in cols if c in sched_df.columns]
        if available:
            result = result.merge(
                sched_df[["IDRSSD"] + available],
                on="IDRSSD",
                how="outer",
            )

    result = result.rename(columns={"IDRSSD": "rssd9001"})
    result["rssd9999"] = pd.to_datetime(date_str, format="%m%d%Y")
    result.columns = [c.lower() if c.startswith("RC") else c for c in result.columns]

    return result


def _dedupe_bank_quarter(df: pd.DataFrame, name: str = "") -> pd.DataFrame:
    """Deduplicate a bank-quarter panel to one row per bank and quarter.

    Drops exact duplicate rows and, if necessary, collapses duplicate
    (rssd9001, rssd9999) keys by keeping the last row within each
    bank-quarter.

    Parameters
    ----------
    df : pd.DataFrame
        Bank-quarter panel to deduplicate.
    name : str
        Optional dataset name used in status messages.

    Returns
    -------
    pd.DataFrame
    """
    key = ["rssd9001", "rssd9999"]

    exact_dups = int(df.duplicated().sum())
    if exact_dups:
        print(f"{name}: dropping {exact_dups} exact duplicate rows")
        df = df.drop_duplicates()

    key_dups = int(df.duplicated(subset=key).sum())
    if key_dups:
        print(f"{name}: dropping {key_dups} duplicate bank-quarter rows (keeping last)")
        df = df.sort_values(key).drop_duplicates(subset=key, keep="last")

    return df


# ---------------------------------------------------------------------------
# Pull functions — construct FFIEC panels from local raw text files
# ---------------------------------------------------------------------------


def pull_rcon_series_1(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic balance sheet series 1 from FFIEC Call Reports.

    Fetches RMBS by maturity (rcona555, rcona557), first-lien residential
    mortgages by maturity (rcona564-569), uninsured deposits (rcon5597),
    and insured deposits (rconf049, rconf045) from FFIEC bulk Call Report
    schedules (RCB, RCCI, and RCO).

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
"""
    cols = {
        "RCB": ["RCONA555", "RCONA557"],
        "RCCI": ["RCONA564", "RCONA565", "RCONA566", "RCONA567", "RCONA568", "RCONA569"],
        "RCO": ["RCON5597", "RCONF049", "RCONF045"],
    }

    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)

    col_order = [
        "rssd9001", "rssd9999",
        "rcona555", "rcona557",
        "rcona564", "rcona565", "rcona566", "rcona567", "rcona568", "rcona569",
        "rcon5597", "rconf049", "rconf045",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()


def pull_rcon_series_2(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic balance sheet series 2 from FFIEC Call Reports.

    Fetches treasury and other securities by maturity (rcona549-554),
    additional maturity buckets (rcona556, rcona558-560), other loans by
    maturity (rcona570-575), and total assets (rcon2170) from FFIEC bulk
    Call Report schedules (RCB, RCCI, and RC).

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    cols = {
        "RCB": ["RCONA549", "RCONA550", "RCONA551", "RCONA552", "RCONA553", "RCONA554",
                "RCONA556", "RCONA558", "RCONA559", "RCONA560"],
        "RCCI": ["RCONA570", "RCONA571", "RCONA572", "RCONA573", "RCONA574", "RCONA575"],
        "RC": ["RCON2170"],
    }

    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)

    col_order = [
        "rssd9001", "rssd9999",
        "rcona549", "rcona550", "rcona551", "rcona552", "rcona553", "rcona554",
        "rcona556", "rcona558", "rcona559", "rcona560",
        "rcona570", "rcona571", "rcona572", "rcona573", "rcona574", "rcona575",
        "rcon2170",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()


def pull_rcfd_series_1(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic+foreign balance sheet series 1 from FFIEC Call Reports.

    Fetches RMBS by maturity (rcfda555-560) and cash balances (rcfd0010)
    from FFIEC bulk Call Report schedules (RCB and RCA).

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    cols = {
        "RCB": ["RCFDA555", "RCFDA556", "RCFDA557", "RCFDA558", "RCFDA559", "RCFDA560"],
        "RCA": ["RCFD0010"],
    }

    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)

    col_order = [
        "rssd9001", "rssd9999",
        "rcfda555", "rcfda556", "rcfda557", "rcfda558", "rcfda559", "rcfda560",
        "rcfd0010",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()


def pull_rcfd_series_2(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic+foreign balance sheet series 2 from FFIEC Call Reports.

    Fetches treasury and other securities by maturity (rcfda549-554) and
    total assets (rcfd2170) from FFIEC bulk Call Report schedules (RCB and RC).

    Parameters
    ----------
    start_date : str or datetime
        Start of date range filter on rssd9999 (report date).
    end_date : str or datetime
        End of date range filter on rssd9999.
    data_dir : Path or str
        Directory containing extracted FFIEC bulk schedule files.

    Returns
    -------
    pd.DataFrame
    """
    cols = {
        "RCB": ["RCFDA549", "RCFDA550", "RCFDA551", "RCFDA552", "RCFDA553", "RCFDA554"],
        "RC": ["RCFD2170"],
    }

    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)

    col_order = [
        "rssd9001", "rssd9999",
        "rcfda549", "rcfda550", "rcfda551", "rcfda552", "rcfda553", "rcfda554",
        "rcfd2170",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()


# ---------------------------------------------------------------------------
# Load functions — read cached parquet files
# ---------------------------------------------------------------------------


def load_rcon_series_1(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 1 from _data/RCON_Series_1_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_1_ffiec.parquet")


def load_rcon_series_2(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 1 from _data/RCON_Series_2_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_2_ffiec.parquet")


def load_rcfd_series_1(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 1 from _data/RCFD_Series_1_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_1_ffiec.parquet")


def load_rcfd_series_2(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 1 from _data/RCFD_Series_2_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_2_ffiec.parquet")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import sys

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    FFIEC_RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Pull raw FFIEC bulk data from the internet.
    # Skip this step with --skip-download if raw files already exist locally.
    if "--skip-download" not in sys.argv:
        print("=== Step 1: Pulling FFIEC bulk data ===")
        pull_call_report_single_period(
            data_dir=FFIEC_RAW_DIR,
            start_year=START_YEAR,
            end_year=END_YEAR,
        )
    else:
        print("=== Step 1: Skipping raw pull (using existing local FFIEC files) ===")

    # Step 2: Build cleaned FFIEC panel datasets from local raw files
    # and cache them as parquet outputs for downstream analysis.
    print("\n=== Step 2: Building and saving FFIEC parquet files ===")

    for name, pull_fn in [
        ("RCON_Series_1_ffiec", pull_rcon_series_1),
        ("RCON_Series_2_ffiec", pull_rcon_series_2),
        ("RCFD_Series_1_ffiec", pull_rcfd_series_1),
        ("RCFD_Series_2_ffiec", pull_rcfd_series_2),
    ]:
        df = pull_fn()
        df = _dedupe_bank_quarter(df, name=name)
        df.to_parquet(DATA_DIR / f"{name}.parquet")
        print(f"{name}: {len(df):,} rows saved")

