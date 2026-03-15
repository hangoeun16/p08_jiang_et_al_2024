"""Run or update the project. This file uses the `doit` Python package (like make).

Pipeline stages:
  1. config       — create _data/ and _output/ directories
  2. pull:wrds    — pull WRDS Call Report data, save to _data/*.parquet
  3. pull:etf     — pull ETF prices via yfinance, save to _data/etf_prices.parquet
  4. analysis     — run full MTM loss analysis, save tables/figure data to _data/
  5. outputs      — generate Table 1, Table A1, and Figure A1 files in _output/
  6. notebooks    — convert .py percent notebooks to .ipynb, execute, export to HTML
  7. latex        — compile LaTeX report via latexmk

Run all tasks:
    doit

Run a specific task:
    doit pull:wrds
    doit analysis
"""

import sys

sys.path.insert(1, "./src/")

from pathlib import Path

from settings import config

DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

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
# Tasks
# ---------------------------------------------------------------------------


def task_config():
    """Create _data/ and _output/ directories if they don't exist."""
    return {
        "actions": ["ipython ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull raw data from external sources."""
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

    yield {
        "name": "struct_rel",
        "doc": "Pull WRDS structural relationship parquet for GSIB mapping",
        "actions": [
            "ipython ./src/settings.py",
            "ipython ./src/pull_struct_rel_2022.py",
        ],
        "targets": [DATA_DIR / "struct_rel_2022.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_struct_rel_2022.py"],
        "clean": [],
    }


def task_analysis():
    """Run full MTM loss analysis and save results to _data/."""
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


def task_outputs():
    """Generate LaTeX tables and figures from analysis results."""
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
        ],
        "clean": True,
    }
