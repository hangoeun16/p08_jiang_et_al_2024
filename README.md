# Monetary Tightening and U.S. Bank Fragility — Replication

Replicates **Table 1**, **Table A1**, and **Figure A1** from:

> Jiang, E., Matvos, G., Piskorski, T., & Seru, A. (2024). "Monetary Tightening and U.S. Bank Fragility in 2023: Mark-to-Market Losses and Uninsured Depositor Runs?" *Journal of Finance*.

The pipeline uses Q1 2022 WRDS Call Report balance sheet data and iShares ETF price changes (Q1 2022 → Q1 2023) to compute mark-to-market (MTM) losses for all U.S. commercial banks, classified by size: Small, Large non-GSIB, and GSIB. It also produces original analysis: an ETF price change summary table and a bank fragility scatter plot.

**Team members:** Summer Han, Joe Wang (FINM32900 Final Project)

---

## Team Responsibilities

| Task | Joe Wang | Summer Han |
|------|----------|------------|
| refactoring previous group's code | X |  |
| write up tex file | X | |
| create tex file pipeline | X | |
| data tour and summary stats | X | |
| web scraping off FFIEC | | X |
| create pipeline for data extension from 2023-2025 | | X |
| fix WRDS data queries | | X |

---

## Quick Start

### 1. Create the environment

```bash
conda env create -f environment.yml
conda activate p08_jiang_et_al_2024
pip install -r requirements.txt
```

You must also have TeX Live (or another LaTeX distribution) installed for the final PDF report ([macOS](https://tug.org/mactex/mactex-download.html), [Windows](https://tug.org/texlive/windows.html#install)).

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env and set WRDS_USERNAME=your_wrds_username
```

### 3. Run the full pipeline

```bash
doit
```

---

## Pipeline Stages

| Stage | Description |
|-------|-------------|
| `doit config` | Create `_data/` and `_output/` directories |
| `doit pull:wrds` | Pull WRDS Call Report data (RCON/RCFD/RCFN series) |
| `doit pull:etf` | Pull iShares ETF prices via yfinance |
| `doit pull:struct_rel` | Pull WRDS relation data |
| `doit analysis` | Compute MTM losses, save results to `_data/` |
| `doit outputs` | Generate LaTeX tables, figures, and original analysis in `_output/` |
| `doit convert_notebooks` | Convert `.py` percent notebooks → `.ipynb` via jupytext |
| `doit run_notebooks` | Execute notebooks and export to HTML |
| `doit compile_latex` | Build PDF report via latexmk |

---

## Project Structure

```
p08_jiang_et_al_2024/
├── src/
│   ├── settings.py              # Configuration (DATA_DIR, WRDS_USERNAME, dates)
│   │
│   ├── pull_wrds.py             # Pull WRDS Call Report data (4 series)
│   ├── pull_ffiec.py            # Pull FFIEC Call Report data (4 series)
│   ├── pull_etf_data.py         # Pull iShares ETF prices via yfinance
│   ├── pull_struct_rel_2022.py  # Pull WRDS relation data 
│   │
│   ├── clean_data.py            # Extract tidy per-bank balance sheet items
│   ├── calc_mtm_losses.py       # Core MTM loss methodology (RMBS multiplier, losses)
│   ├── calc_table1.py           # Aggregate Table 1 statistics by bank size
│   ├── calc_summary_stats.py    # Balance sheet composition for Table A1 / Figure A1
│   │
│   ├── run_analysis.py          # Orchestrates full analysis pipeline
│   │
│   ├── create_table1.py         # Generate Table 1 as LaTeX (.tex)
│   ├── create_table_a1.py       # Generate Table A1 as LaTeX (.tex)
│   ├── create_figure_a1.py      # Generate Figure A1 as PDF/PNG
│   ├── create_etf_table.py      # Generate ETF price change summary table as LaTeX (.tex)
│   ├── create_fragility_figure.py  # Generate bank fragility scatter plot as PDF/PNG
│   │
│   ├── 01_data_tour.py          # Notebook: data overview (percent format → .ipynb)
│   ├── 02_replication.py        # Notebook: replication results + updated analysis
│   │
│   ├── test_calc_mtm_losses.py  # Unit tests for MTM calculation (21 tests)
│   └── test_clean_data.py       # Unit tests for data cleaning
│
├── reports/
│   ├── main.tex                 # LaTeX report (inputs tables/figures from _output/)
│   ├── bibliography.bib         # References
│   └── my_article_header.sty   # LaTeX style
│
├── _data/                       # Auto-generated data cache (gitignored)
├── _output/                     # Auto-generated tables, figures, notebooks (gitignored)
├── data_manual/                 # Version-controlled manual data (if any)
│
├── dodo.py                      # PyDoit task runner (like a Makefile)
├── .env.example                 # Template for .env credentials
├── .env                         # Local credentials (gitignored — never commit)
├── requirements.txt             # pip dependencies
├── environment.yml              # conda environment
└── pyproject.toml               # pytest configuration
```

---

## Data Sources

| Data | Source | How pulled |
|------|--------|------------|
| Call Report balance sheets | WRDS (`bank.wrds_call_rcon_1/2`, `rcfd_1/2`) | `pull_wrds.py` via `wrds` Python package |
| iShares Treasury ETFs (SHV, SHY, IEI, IEF, TLH, TLT) | Yahoo Finance | `pull_etf_data.py` via `yfinance` |
| iShares MBS ETF (MBB) | Yahoo Finance | `pull_etf_data.py` via `yfinance` |
| S&P Treasury Bond Index proxy (GOVT) | Yahoo Finance | `pull_etf_data.py` via `yfinance` |

WRDS access requires an institutional subscription. Set `WRDS_USERNAME` in `.env`.

---

## Configuration

All settings are managed via `src/settings.py` and a `.env` file in the project root. Copy `.env.example` to `.env` and fill in your values:

```
WRDS_USERNAME=your_wrds_username
START_DATE=2021-12-31       # WRDS pull range start
END_DATE=2023-09-30         # WRDS pull range end
FFIEC_START_DATE=2023-12-31       # FFIEC pull range start
FFIEC_END_DATE=2025-12-31         # FFIEC pull range end
REPORT_DATE=2022-03-31      # Balance sheet snapshot (Q1 2022 per paper)
MTM_END_DATE=2023-03-31     # MTM loss measurement end (Q1 2023 per paper)
```

Override `DATA_DIR` or `OUTPUT_DIR` to store data/output in a custom location.

---

## Running Tests

```bash
pytest
```

Tests in `src/test_calc_mtm_losses.py` and `src/test_clean_data.py` use synthetic inputs to verify the MTM calculation logic and data cleaning functions. Four tolerance-based integration tests (skipped if data is not cached) verify that aggregate results match the paper's Table 1 values within stated tolerances:

| Test | Paper value | Tolerance |
|------|-------------|-----------|
| Aggregate MTM loss | $2,200B | 40% |
| Total bank count | 4,844 | 2% |
| Median loss/assets | 9.2% | 25% |
| Small bank share | ~84% | 75–95% range |

---

## Formatting

```bash
ruff format .               # format code
ruff check . --fix          # fix linting issues
```

---

## Naming Conventions

- **`pull_*.py`** — fetches from external source; saves parquet to `_data/`; includes matching `load_*()` functions
- **`clean_data.py`** — transforms raw WRDS data into tidy per-bank DataFrames
- **`calc_*.py`** — pure computation functions (no I/O)
- **`create_*.py`** — generates output artifacts (LaTeX tables, PDF/PNG figures)
- **`test_*.py`** — pytest unit tests

## Directory Notes

- `_data/` and `_output/` are **gitignored** — they are fully reproducible by running `doit`
- `data_manual/` is **version controlled** — for any data that cannot be automatically re-downloaded
- `.env` must **never** be committed — use `.env.example` as the template
