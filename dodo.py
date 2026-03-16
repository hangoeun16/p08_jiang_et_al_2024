"""Run or update the project. This file uses the `doit` Python package (like make).

Pipeline stages:
  1. config            — create _data/ and _output/ directories

  WRDS track (original Jiang et al. replication, 2021 Q4 – 2023 Q3):
  2. pull:wrds         — pull WRDS Call Report data, save to _data/*.parquet
  3. pull:struct_rel   — pull structural relationship (2022) for GSIB mapping
  4. analysis          — run MTM loss analysis on WRDS data
  5. outputs           — generate Table 1, Table A1, Figure A1 from WRDS results

  FFIEC track (extension, 2023 Q4 – 2025 Q4):
  6. pull:ffiec        — pull FFIEC Call Report data, save to _data/*_ffiec.parquet
  7. pull:struct_rel_ffiec — pull structural relationship (2024) for GSIB mapping
  8. analysis_ffiec    — run MTM loss analysis on FFIEC data
  9. outputs_ffiec     — generate Table 1, Table A1, Figure A1 from FFIEC results

  Shared:
  10. pull:etf          — pull ETF prices via yfinance (used by both tracks)
  11. convert_notebooks — convert .py percent notebooks → .ipynb via jupytext
  12. run_notebooks     — execute notebooks and export to HTML
  13. compile_latex     — build PDF report via latexmk

Run all tasks:
    doit

Run only WRDS track:
    doit pull:wrds pull:etf pull:struct_rel analysis outputs

Run only FFIEC track:
    doit ffiec
"""

import sys

sys.path.insert(1, "./src/")

from pathlib import Path

from settings import config

DOIT_CONFIG = {
    "backend": "sqlite3",
    "dep_file": "./.doit-db.sqlite",
    "default_tasks": [
        "config",
        "pull:wrds",
        "pull:etf",
        "pull:struct_rel",
        "analysis",
        "outputs:table1",
        "outputs:table_a1",
        "outputs:figure_a1",
        "outputs:table_etf",
        "outputs:figure_fragility",
        "convert_notebooks",
        "run_notebooks",
        "compile_latex",
    ],
}

DATA_DIR = config("DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# ---------------------------------------------------------------------------
# Jupyter notebook helpers
# ---------------------------------------------------------------------------


def jupyter_execute_notebook(nb):
    return (
        f"jupyter nbconvert --execute --to notebook "
        f"--ClearMetadataPreprocessor.enabled=True --inplace {nb}"
    )


def jupyter_to_html(nb, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {nb}"


def jupyter_clear_output(nb):
    return (
        f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True "
        f"--ClearMetadataPreprocessor.enabled=True --inplace {nb}"
    )


def jupytext_to_notebook(py_file):
    """Convert a percent-format .py file to a .ipynb notebook."""
    return f"jupytext --to notebook {py_file}"


# ---------------------------------------------------------------------------
# Tasks — Configuration
# ---------------------------------------------------------------------------


def task_config():
    """Create _data/ and _output/ directories if they don't exist."""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# Tasks — Data pulling
# ---------------------------------------------------------------------------


def task_pull():
    """Pull raw data from external sources."""

    # ---- WRDS Call Reports (original replication) ----
    yield {
        "name": "wrds",
        "doc": "Pull WRDS Call Report data (RCON/RCFD/RCFN series 1 and 2)",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_wrds.py",
        ],
        "targets": [
            DATA_DIR / "RCON_Series_1.parquet",
            DATA_DIR / "RCON_Series_2.parquet",
            DATA_DIR / "RCFD_Series_1.parquet",
            DATA_DIR / "RCFD_Series_2.parquet",
            DATA_DIR / "RCFN_Series_1.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_wrds.py"],
        "clean": [],
    }

    # ---- FFIEC Call Reports (extension) ----
    yield {
        "name": "ffiec",
        "doc": "Pull FFIEC Call Report data (RCON/RCFD/RCFN series, 2023-2025)",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_ffiec.py",
        ],
        "targets": [
            DATA_DIR / "RCON_Series_1_ffiec.parquet",
            DATA_DIR / "RCON_Series_2_ffiec.parquet",
            DATA_DIR / "RCFD_Series_1_ffiec.parquet",
            DATA_DIR / "RCFD_Series_2_ffiec.parquet",
            DATA_DIR / "RCFN_Series_1_ffiec.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_ffiec.py"],
        "clean": [],
    }

    # ---- ETF prices (shared by both tracks) ----
    yield {
        "name": "etf",
        "doc": "Pull Treasury and MBS ETF prices via yfinance",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_etf_data.py",
        ],
        "targets": [DATA_DIR / "etf_prices.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_etf_data.py"],
        "clean": [],
    }

    # ---- Structural relationships (2022, for WRDS track) ----
    yield {
        "name": "struct_rel",
        "doc": "Pull structural relationship parquet for GSIB mapping (2022)",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_struct_rel.py",
        ],
        "targets": [DATA_DIR / "struct_rel_2022.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_struct_rel.py"],
        "clean": [],
    }

    # ---- Structural relationships (2024, for FFIEC track) ----
    yield {
        "name": "struct_rel_ffiec",
        "doc": "Pull structural relationship parquet for GSIB mapping (2024)",
        "actions": [
            "ipython ./src/settings.py",
            "python ./src/pull_struct_rel.py --year 2024",
        ],
        "targets": [DATA_DIR / "struct_rel_2024.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_struct_rel.py"],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# Tasks — Analysis
# ---------------------------------------------------------------------------


def task_analysis():
    """Run full MTM loss analysis on WRDS data and save results to _data/."""
    return {
        "actions": ["ipython ./src/run_analysis.py"],
        "targets": [
            DATA_DIR / "bank_losses.parquet",
            DATA_DIR / "uninsured_ratio.parquet",
            DATA_DIR / "insured_coverage.parquet",
            DATA_DIR / "table1.parquet",
            DATA_DIR / "table_a1_panel_a.parquet",
            DATA_DIR / "table_a1_panel_b.parquet",
            DATA_DIR / "figure_a1_data.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/run_analysis.py",
            "./src/clean_data.py",
            "./src/calc_mtm_losses.py",
            "./src/calc_table1.py",
            "./src/calc_summary_stats.py",
            DATA_DIR / "RCON_Series_1.parquet",
            DATA_DIR / "RCON_Series_2.parquet",
            DATA_DIR / "RCFD_Series_1.parquet",
            DATA_DIR / "RCFD_Series_2.parquet",
            DATA_DIR / "RCFN_Series_1.parquet",
            DATA_DIR / "etf_prices.parquet",
            DATA_DIR / "struct_rel_2022.parquet",
        ],
        "clean": [],
    }


def task_analysis_ffiec():
    """Run full MTM loss analysis on FFIEC data and save results to _data/."""
    return {
        "actions": ["python ./src/run_analysis.py --source ffiec"],
        "targets": [
            DATA_DIR / "bank_losses_ffiec.parquet",
            DATA_DIR / "uninsured_ratio_ffiec.parquet",
            DATA_DIR / "insured_coverage_ffiec.parquet",
            DATA_DIR / "table1_ffiec.parquet",
            DATA_DIR / "table_a1_panel_a_ffiec.parquet",
            DATA_DIR / "table_a1_panel_b_ffiec.parquet",
            DATA_DIR / "figure_a1_data_ffiec.parquet",
        ],
        "file_dep": [
            "./src/settings.py",
            "./src/run_analysis.py",
            "./src/clean_data.py",
            "./src/calc_mtm_losses.py",
            "./src/calc_table1.py",
            "./src/calc_summary_stats.py",
            DATA_DIR / "RCON_Series_1_ffiec.parquet",
            DATA_DIR / "RCON_Series_2_ffiec.parquet",
            DATA_DIR / "RCFD_Series_1_ffiec.parquet",
            DATA_DIR / "RCFD_Series_2_ffiec.parquet",
            DATA_DIR / "RCFN_Series_1_ffiec.parquet",
            DATA_DIR / "etf_prices.parquet",
            DATA_DIR / "struct_rel_2024.parquet",
        ],
        "clean": [],
    }


# ---------------------------------------------------------------------------
# Tasks — Outputs (tables, figures)
# ---------------------------------------------------------------------------


def task_outputs():
    """Generate LaTeX tables and figures from WRDS analysis results."""
    yield {
        "name": "table1",
        "doc": "Generate Table 1 LaTeX file",
        "actions": ["ipython ./src/create_table1.py"],
        "targets": [OUTPUT_DIR / "table1.tex"],
        "file_dep": [
            "./src/create_table1.py",
            DATA_DIR / "table1.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "table_a1",
        "doc": "Generate Table A1 LaTeX file",
        "actions": ["ipython ./src/create_table_a1.py"],
        "targets": [OUTPUT_DIR / "table_a1.tex"],
        "file_dep": [
            "./src/create_table_a1.py",
            DATA_DIR / "table_a1_panel_a.parquet",
            DATA_DIR / "table_a1_panel_b.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "figure_a1",
        "doc": "Generate Figure A1 PDF and PNG",
        "actions": ["ipython ./src/create_figure_a1.py"],
        "targets": [
            OUTPUT_DIR / "figure_a1.pdf",
            OUTPUT_DIR / "figure_a1.png",
        ],
        "file_dep": [
            "./src/create_figure_a1.py",
            DATA_DIR / "figure_a1_data.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "table_etf",
        "doc": "Generate ETF price change summary table",
        "actions": ["ipython ./src/create_etf_table.py"],
        "targets": [OUTPUT_DIR / "table_etf.tex"],
        "file_dep": [
            "./src/create_etf_table.py",
            DATA_DIR / "etf_prices.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "figure_fragility",
        "doc": "Generate bank fragility scatter plot",
        "actions": ["ipython ./src/create_fragility_figure.py"],
        "targets": [
            OUTPUT_DIR / "figure_fragility.pdf",
            OUTPUT_DIR / "figure_fragility.png",
        ],
        "file_dep": [
            "./src/create_fragility_figure.py",
            DATA_DIR / "bank_losses.parquet",
            DATA_DIR / "uninsured_ratio.parquet",
        ],
        "clean": True,
    }


def task_outputs_ffiec():
    """Generate LaTeX tables and figures from FFIEC analysis results."""
    yield {
        "name": "table1",
        "doc": "Generate Table 1 LaTeX file (FFIEC)",
        "actions": ["python ./src/create_table1.py --source ffiec"],
        "targets": [OUTPUT_DIR / "table1_ffiec.tex"],
        "file_dep": [
            "./src/create_table1.py",
            DATA_DIR / "table1_ffiec.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "table_a1",
        "doc": "Generate Table A1 LaTeX file (FFIEC)",
        "actions": ["python ./src/create_table_a1.py --source ffiec"],
        "targets": [OUTPUT_DIR / "table_a1_ffiec.tex"],
        "file_dep": [
            "./src/create_table_a1.py",
            DATA_DIR / "table_a1_panel_a_ffiec.parquet",
            DATA_DIR / "table_a1_panel_b_ffiec.parquet",
        ],
        "clean": True,
    }

    yield {
        "name": "figure_a1",
        "doc": "Generate Figure A1 PDF and PNG (FFIEC)",
        "actions": ["python ./src/create_figure_a1.py --source ffiec"],
        "targets": [
            OUTPUT_DIR / "figure_a1_ffiec.pdf",
            OUTPUT_DIR / "figure_a1_ffiec.png",
        ],
        "file_dep": [
            "./src/create_figure_a1.py",
            DATA_DIR / "figure_a1_data_ffiec.parquet",
        ],
        "clean": True,
    }


# ---------------------------------------------------------------------------
# Tasks — Notebooks
# ---------------------------------------------------------------------------


def task_convert_notebooks():
    """Convert percent-format .py notebooks to .ipynb using jupytext."""
    notebooks = [
        "src/01_data_tour.py",
        "src/02_replication.py",
    ]
    for py_file in notebooks:
        nb_file = py_file.replace(".py", ".ipynb")
        yield {
            "name": py_file,
            "doc": f"Convert {py_file} → {nb_file}",
            "actions": [jupytext_to_notebook(py_file)],
            "targets": [nb_file],
            "file_dep": [py_file],
            "clean": True,
        }


def task_run_notebooks():
    """Execute and export Jupyter notebooks to HTML."""
    notebooks = [
        "src/01_data_tour.ipynb",
        "src/02_replication.ipynb",
    ]

    # Sequential execution: each notebook depends on the previous
    prev_nb = None
    for nb in notebooks:
        py_file = nb.replace(".ipynb", ".py")
        file_dep = [
            nb,
            py_file,
            DATA_DIR / "bank_losses.parquet",
        ]
        if prev_nb:
            file_dep.append(OUTPUT_DIR / (Path(prev_nb).stem + ".html"))

        yield {
            "name": nb,
            "doc": f"Execute and export {nb}",
            "actions": [
                jupyter_execute_notebook(nb),
                jupyter_to_html(nb),
            ],
            "targets": [OUTPUT_DIR / (Path(nb).stem + ".html")],
            "file_dep": file_dep,
            "clean": True,
        }
        prev_nb = nb


# ---------------------------------------------------------------------------
# Tasks — LaTeX
# ---------------------------------------------------------------------------


def task_compile_latex():
    """Compile the LaTeX report using latexmk."""
    latex_file = "./reports/main.tex"
    return {
        "actions": [
            f"latexmk -xelatex -cd -quiet {latex_file}",
        ],
        "targets": ["./reports/main.pdf"],
        "file_dep": [
            latex_file,
            OUTPUT_DIR / "table1.tex",
            OUTPUT_DIR / "table_a1.tex",
            OUTPUT_DIR / "figure_a1.pdf",
            OUTPUT_DIR / "table_etf.tex",
            OUTPUT_DIR / "figure_fragility.pdf",
        ],
        "clean": True,
    }


def task_ffiec():
    """Run the entire FFIEC extension pipeline (pull → analysis → outputs)."""
    return {
        "actions": None,
        "task_dep": [
            "config",
            "pull:ffiec",
            "pull:etf",
            "pull:struct_rel_ffiec",
            "analysis_ffiec",
            "outputs_ffiec:table1",
            "outputs_ffiec:table_a1",
            "outputs_ffiec:figure_a1",
        ],
    }