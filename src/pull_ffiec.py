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
  - RCFN_Series_1_ffiec: foreign office deposits from wrds_call_rcfn_1


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
import time
import zipfile
from pathlib import Path
from settings import config
 
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
 
 
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
 
DATA_DIR = Path(config("DATA_DIR"))
FFIEC_RAW_DIR = Path(config("FFIEC_RAW_DIR", default="ffiec_data"))
FFIEC_RAW_DIR.mkdir(parents=True, exist_ok=True)
 
# Panel-construction window
FFIEC_START_DATE = config("FFIEC_START_DATE")
FFIEC_END_DATE = config("FFIEC_END_DATE")
START_YEAR = config("FFIEC_START_YEAR")
END_YEAR = config("FFIEC_END_YEAR")
 
FFIEC_BASE_URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
DOWNLOAD_TIMEOUT = config("FFIEC_DOWNLOAD_TIMEOUT")
 
 
# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------
 
 
def _wait_for_download(data_dir: Path, timeout: int = DOWNLOAD_TIMEOUT) -> None:
    """Wait for Chrome to finish downloading files into `data_dir`.
 
    Sleeps briefly to let the browser start the download, then polls for
    temporary `.crdownload` and `.part` files until all downloads complete
    or the timeout is reached.
 
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
    time.sleep(2)  # give browser time to start the download before polling
    start = time.time()
    while time.time() - start < timeout:
        partials = list(data_dir.glob("*.crdownload")) + list(data_dir.glob("*.part"))
        zip_files = list(data_dir.glob("*.zip"))
        if not partials and zip_files:
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
    year range from the FFIEC CDR bulk data page and extracts them into
    the target directory. For the start year, only Q4 (12/31) is downloaded
    to match the panel window beginning at FFIEC_START_DATE.
 
    The page uses ASP.NET postbacks: selecting the product (ListBox1)
    populates the date dropdown (DatesDropDownList). Dates are displayed
    as MM/DD/YYYY (e.g. 03/31/2023). Format is Tab Delimited (TSVRadioButton).
 
    Parameters
    ----------
    data_dir : Path
        Directory where downloaded and extracted FFIEC files will be stored.
    start_year : int
        First year to download. Only Q4 is downloaded for this year.
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
        wait = WebDriverWait(driver, 20)
 
        for year in range(start_year, end_year + 1):
            # For the start year, only download Q4 to match FFIEC_START_DATE
            quarters = ["1231"] if year == start_year else ["0331", "0630", "0930", "1231"]
 
            for quarter_code in quarters:
                mm, dd = quarter_code[:2], quarter_code[2:]
                period_label = f"{mm}/{dd}/{year}"
                period_key = f"{quarter_code}{year}"
 
                try:
                    # Step 1: Select product — triggers postback to populate dates
                    product_el = wait.until(EC.element_to_be_clickable((By.ID, "ListBox1")))
                    Select(product_el).select_by_value("ReportingSeriesSinglePeriod")
                    time.sleep(2)
 
                    # Step 2: Select reporting period end date
                    date_el = wait.until(EC.element_to_be_clickable((By.ID, "DatesDropDownList")))
                    Select(date_el).select_by_visible_text(period_label)
                    time.sleep(1)
 
                    # Step 3: Select Tab Delimited format
                    tsv_el = wait.until(EC.element_to_be_clickable((By.ID, "TSVRadioButton")))
                    if not tsv_el.is_selected():
                        tsv_el.click()
                    time.sleep(1)
 
                    # Step 4: Click Download
                    download_btn = wait.until(EC.element_to_be_clickable((By.ID, "Download_0")))
                    download_btn.click()
 
                    print(f"  Downloading FFIEC bulk file for {period_label} ...")
                    _wait_for_download(data_dir)
                    time.sleep(1)
 
                except Exception as exc:
                    print(f"  Skipping {period_key} due to error: {exc}")
 
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
    quarter_ends = pd.date_range(start=start_date, end=end_date, freq="QE")
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
        if c not in ("IDRSSD", "RSSD9017"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
 
    return df.reset_index(drop=True)
 
 
def load_ent(date_str: str, data_dir: Path = FFIEC_RAW_DIR) -> pd.DataFrame:
    """Load the ENT schedule for a given FFIEC reporting quarter.
 
    Loads the ENT schedule corresponding to the requested quarter and returns
    the cleaned identifier backbone (IDRSSD and RSSD9017) used to construct
    bank-quarter panels.
 
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
    cols = [c for c in ["IDRSSD", "RSSD9017"] if c in df.columns]
    return df[cols].copy()
 
 
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
    result.columns = [
        c.lower() if c.startswith(("RC", "RSSD")) else c
        for c in result.columns
    ]
 
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
    insured deposits (rconf049, rconf045), and additional balance sheet
    items from schedules RC, RCB, RCCI, and RCO.
 
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
        "RC": [
            "RCON0071", "RCON1773", "RCONB987", "RCONB993", "RCON3230",
        ],
        "RCB": [
            "RCONA555", "RCONA557",
            "RCON1743", "RCON1746", "RCONC988", "RCONC027",
            "RCONG309", "RCONG311", "RCONG313", "RCONG315", "RCONG317",
            "RCONG319", "RCONG321", "RCONG323",
            "RCONK143", "RCONK145", "RCONK147", "RCONK149", "RCONK151",
            "RCONK153", "RCONK155", "RCONK157",
            "RCONHT55", "RCONHT57", "RCONHT59", "RCONHT61",
        ],
        "RCCI": [
            "RCONA564", "RCONA565", "RCONA566", "RCONA567", "RCONA568", "RCONA569",
            "RCONB538", "RCONK137", "RCONK207", "RCONJ454", "RCONJ451",
            "RCONF158", "RCONF159", "RCONF160", "RCONF161",
            "RCON5367", "RCON5368", "RCON1590", "RCON1766",
        ],
        "RCO": [
            "RCON5597", "RCONF049", "RCONF045",
        ],
        "RCE": [
            "RCONMT91", "RCONMT87", "RCONHK14", "RCONHK15",
        ],
    }
 
    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)
 
    col_order = [
        "rssd9001", "rssd9017", "rssd9999",
        "rcon0071", "rcon1773", "rconb987", "rconb993", "rcon3230",
        "rcona555", "rcona557",
        "rcon1743", "rcon1746", "rconc988", "rconc027",
        "rcong309", "rcong311", "rcong313", "rcong315", "rcong317",
        "rcong319", "rcong321", "rcong323",
        "rconk143", "rconk145", "rconk147", "rconk149", "rconk151",
        "rconk153", "rconk155", "rconk157",
        "rconht55", "rconht57", "rconht59", "rconht61",
        "rcona564", "rcona565", "rcona566", "rcona567", "rcona568", "rcona569",
        "rconb538", "rconk137", "rconk207", "rconj454", "rconj451",
        "rconf158", "rconf159", "rconf160", "rconf161",
        "rcon5367", "rcon5368", "rcon1590", "rcon1766",
        "rcon5597", "rconf049", "rconf045",
        "rconmt91", "rconmt87", "rconhk14", "rconhk15",
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
    maturity (rcona570-575), total assets (rcon2170), and additional
    balance sheet items from schedules RC, RCB, and RCCI.
 
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
        "RC": [
            "RCON0081", "RCON1771", "RCON0213", "RCON1287",
            "RCON1738", "RCON1741", "RCON2122", "RCON1420",
            "RCON1797", "RCON1460", "RCONB989", "RCON2200",
            "RCON2948", "RCON2930", "RCONG105", "RCON3838", "RCON3632", "RCON2170",
        ],
        "RCB": [
            "RCONA549", "RCONA550", "RCONA551", "RCONA552", "RCONA553", "RCONA554",
            "RCONA556", "RCONA558", "RCONA559", "RCONA560",
        ],
        "RCCI": [
            "RCONA570", "RCONA571", "RCONA572", "RCONA573", "RCONA574", "RCONA575",
            "RCONB539", "RCONJ464", "RCONB995",
        ],
        "RCE": [
            "RCONHK05", "RCONJ474", "RCONK222",
        ],
    }
 
    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)
 
    col_order = [
        "rssd9001", "rssd9017", "rssd9999",
        "rcon0081", "rcon1771", "rcon0213", "rcon1287",
        "rcon1738", "rcon1741", "rcon2122", "rcon1420",
        "rcon1797", "rcon1460", "rconb989", "rcon2200",
        "rcon2948", "rcon2930", "rcong105", "rcon3838", "rcon3632", "rcon2170",
        "rcona549", "rcona550", "rcona551", "rcona552", "rcona553", "rcona554",
        "rcona556", "rcona558", "rcona559", "rcona560",
        "rcona570", "rcona571", "rcona572", "rcona573", "rcona574", "rcona575",
        "rconb539", "rconj464", "rconb995",
        "rconhk05", "rconj474", "rconk222",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()
 
 
def pull_rcfd_series_1(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic+foreign balance sheet series 1 from FFIEC Call Reports.
 
    Fetches RMBS by maturity (rcfda555-560), cash balances (rcfd0010),
    and additional balance sheet items from schedules RCA, RCB, RCCI, and RC.
 
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
        "RCA": [
            "RCFD0010",
        ],
        "RCB": [
            "RCFDA555", "RCFDA556", "RCFDA557", "RCFDA558", "RCFDA559", "RCFDA560",
            "RCFD1773", "RCFD0213", "RCFD1287", "RCFD1771",
            "RCFD1738", "RCFD1741", "RCFD1743", "RCFD1746",
            "RCFDC988", "RCFDC027",
            "RCFDG301", "RCFDG303", "RCFDG305", "RCFDG307", "RCFDG309", "RCFDG311",
            "RCFDG313", "RCFDG315", "RCFDG317", "RCFDG319", "RCFDG321", "RCFDG323",
            "RCFDK143", "RCFDK145", "RCFDK147", "RCFDK149", "RCFDK151",
            "RCFDK153", "RCFDK155", "RCFDK157",
        ],
        "RCCI": [
            "RCFDB538", "RCFDB539", "RCFDK137", "RCFDK207",
            "RCFDF158", "RCFDF159", "RCFDF160", "RCFDF161",
            "RCFD5367", "RCFD5368", "RCFD1590", "RCFD1763", "RCFD1764",
            "RCFD1420", "RCFD1460", "RCFD1797", "RCFD2122",
        ],
        "RC": [
            "RCFD2930", "RCFD3230", "RCFDB989", "RCFDG105",
            "RCFD3838", "RCFD3632", "RCFD2948", "RCFN2200",
        ],
    }
 
    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)
 
    col_order = [
        "rssd9001", "rssd9017", "rssd9999",
        "rcfd0010",
        "rcfda555", "rcfda556", "rcfda557", "rcfda558", "rcfda559", "rcfda560",
        "rcfd1773", "rcfd0213", "rcfd1287", "rcfd1771",
        "rcfd1738", "rcfd1741", "rcfd1743", "rcfd1746",
        "rcfdc988", "rcfdc027",
        "rcfdg301", "rcfdg303", "rcfdg305", "rcfdg307", "rcfdg309", "rcfdg311",
        "rcfdg313", "rcfdg315", "rcfdg317", "rcfdg319", "rcfdg321", "rcfdg323",
        "rcfdk143", "rcfdk145", "rcfdk147", "rcfdk149", "rcfdk151",
        "rcfdk153", "rcfdk155", "rcfdk157",
        "rcfdb538", "rcfdb539", "rcfdk137", "rcfdk207",
        "rcfdf158", "rcfdf159", "rcfdf160", "rcfdf161",
        "rcfd5367", "rcfd5368", "rcfd1590", "rcfd1763", "rcfd1764",
        "rcfd1420", "rcfd1460", "rcfd1797", "rcfd2122",
        "rcfd2930", "rcfd3230", "rcfdb989", "rcfdg105",
        "rcfd3838", "rcfd3632", "rcfd2948", "rcfn2200",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()
 
 
def pull_rcfd_series_2(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic+foreign balance sheet series 2 from FFIEC Call Reports.
 
    Fetches treasury and other securities by maturity (rcfda549-554), total
    assets (rcfd2170), and additional balance sheet items from schedules
    RC and RCB.
 
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
        "RC": [
            "RCFD1771", "RCFD0213", "RCFD1287", "RCFD2122",
            "RCFD1420", "RCFD1797", "RCFD1460", "RCFDB989",
            "RCFD2948", "RCFDG105", "RCFD3838", "RCFD3632", "RCFD2170",
        ],
        "RCB": [
            "RCFDA549", "RCFDA550", "RCFDA551", "RCFDA552", "RCFDA553", "RCFDA554",
        ],
    }
 
    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)
 
    col_order = [
        "rssd9001", "rssd9017", "rssd9999",
        "rcfd1771", "rcfd0213", "rcfd1287", "rcfd2122",
        "rcfd1420", "rcfd1797", "rcfd1460", "rcfdb989",
        "rcfd2948", "rcfdg105", "rcfd3838", "rcfd3632", "rcfd2170",
        "rcfda549", "rcfda550", "rcfda551", "rcfda552", "rcfda553", "rcfda554",
    ]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()
 
def pull_rcfn_series_1(
    start_date: str = FFIEC_START_DATE,
    end_date: str = FFIEC_END_DATE,
    data_dir: Path = FFIEC_RAW_DIR,
) -> pd.DataFrame:
    """Pull domestic+foreign net loans series 1 from FFIEC Call Reports.

    Fetches net loans and leases (rcfn2200) from FFIEC bulk Call Report
    schedule RC.

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
        "RC": ["RCFN2200"],
    }

    dfs = [_build_single_quarter(date_str, cols, data_dir) for date_str in _quarter_date_strings(start_date, end_date)]
    result = pd.concat(dfs, ignore_index=True)

    col_order = ["rssd9001", "rssd9017", "rssd9999", "rcfn2200"]
    existing = [c for c in col_order if c in result.columns]
    return result[existing].copy()

# ---------------------------------------------------------------------------
# Load functions — read cached parquet files
# ---------------------------------------------------------------------------
 
 
def load_rcon_series_1(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 1 from _data/RCON_Series_1_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_1_ffiec.parquet")
 
 
def load_rcon_series_2(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCON series 2 from _data/RCON_Series_2_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCON_Series_2_ffiec.parquet")
 
 
def load_rcfd_series_1(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCFD series 1 from _data/RCFD_Series_1_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_1_ffiec.parquet")
 
 
def load_rcfd_series_2(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCFD series 2 from _data/RCFD_Series_2_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFD_Series_2_ffiec.parquet")
 
def load_rcfn_series_1(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load cached RCFN series 1 from _data/RCFN_Series_1_ffiec.parquet."""
    return pd.read_parquet(Path(data_dir) / "RCFN_Series_1_ffiec.parquet")
 

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
        ("RCFN_Series_1_ffiec", pull_rcfn_series_1),
    ]:
        df = pull_fn()
        df = _dedupe_bank_quarter(df, name=name)
        df.to_parquet(DATA_DIR / f"{name}.parquet")
        print(f"{name}: {len(df):,} rows saved")