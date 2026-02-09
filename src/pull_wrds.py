"""
Module for Extracting and Loading WRDS Financial Data

This module is responsible for fetching financial data series from the WRDS (Wharton Research Data Services)
database and saving them locally as parquet files for subsequent analysis. It uses SQL queries to extract
the RCFD and RCON data series over a specified date range and provides functions to load locally stored
parquet files for these series.

Imports:
    - pandas: Provides data structures and functions for data manipulation.
    - numpy: Supports numerical operations.
    - wrds: Facilitates connection to the WRDS database.
    - config: Contains configuration settings such as data directories and the WRDS username.
    - pathlib.Path: For handling filesystem paths.

Global Variables:
    - OUTPUT_DIR: Directory for saving output files, sourced from config.
    - DATA_DIR: Directory for accessing data files, sourced from config.
    - WRDS_USERNAME: Username for connecting to the WRDS database, sourced from config.

Functions:
    - pull_RCFD_series_1(wrds_username=WRDS_USERNAME):
          Executes an SQL query to fetch the first RCFD series from WRDS within the date range
          '2021-12-31' to '2023-09-30' and returns the data as a pandas DataFrame.
    - pull_RCON_series_2(wrds_username=WRDS_USERNAME):
          Executes an SQL query to fetch the second RCON series from WRDS within the date range
          '2021-12-31' to '2023-09-30' and returns the data as a pandas DataFrame.
    - pull_RCFD_series_2(wrds_username=WRDS_USERNAME):
          Executes an SQL query to fetch the second RCFD series from WRDS within the date range
          '2021-12-31' to '2023-09-30' and returns the data as a pandas DataFrame.
    - pull_RCON_series_1(wrds_username=WRDS_USERNAME):
          Loads the first RCON series from a local parquet file (used due to missing column information
          when extracting directly from WRDS) and returns it as a pandas DataFrame.
    - load_RCON_series_1(data_dir=DATA_DIR):
          Loads and returns the RCON series 1 data from a local parquet file in the specified data directory.
    - load_RCON_series_2(data_dir=DATA_DIR):
          Loads and returns the RCON series 2 data from a local parquet file in the specified data directory.
    - load_RCFD_series_1(data_dir=DATA_DIR):
          Loads and returns the RCFD series 1 data from a local parquet file in the specified data directory.
    - load_RCFD_series_2(data_dir=DATA_DIR):
          Loads and returns the RCFD series 2 data from a local parquet file in the specified data directory.
    - _demo():
          Demonstrates the loading of all local data series.
    - main():
          Pulls the data series from WRDS using the respective pull functions and saves them locally as
          parquet files for future use.

Usage:
    Run this module as a standalone script to connect to the WRDS database, extract the necessary financial
    data series, and save them as parquet files. These files can later be loaded using the provided load functions
    for further analysis.
"""

import pandas as pd
from pandas.tseries.offsets import MonthEnd, YearEnd

import numpy as np
import wrds

from settings import config
from pathlib import Path


OUTPUT_DIR = Path(config("OUTPUT_DIR"))
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
START_DATE = config("START_DATE")
END_DATE = config("END_DATE")


def pull_RCON_series_1(
        start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """Pull RCON series from WRDS.
    """
    sql_query = f"""
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, rcon5597
        FROM 
            bank.wrds_call_rcon_1 AS b
        WHERE 
            b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcon_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])

    # Close the connection
    db.close()

    return rcon_series_1

def pull_RCON_series_2(
        start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """Pull RCON series from WRDS.
    """
    sql_query = f"""
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcona549,
            b.rcona550, b.rcona551, b.rcona552, b.rcona553, b.rcona554, 
            b.rcona561, b.rcona562, b.rcona570, b.rcona571, b.rcona572, 
            b.rcona573, b.rcona574, b.rcona575, b.rcon2170
        FROM 
            bank.wrds_call_rcon_2 AS b
        WHERE 
            b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcon_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcon_series_2

def pull_RCFD_series_1(
        start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """Pull RCFD series from WRDS.
    """
    sql_query = f"""
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcfda555, b.rcfda247,
            b.rcfda556, b.rcfda557, b.rcfda558, b.rcfda559, b.rcfda560,
            b.rcfda561, b.rcfda562, b.rcfda570, b.rcfda571, b.rcfda572, 
            b.rcfda573, b.rcfda574, b.rcfda575, b.rcfd0010
        FROM 
            bank.wrds_call_rcfd_1 AS b
        WHERE 
            b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcfd_series_1 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_1

def pull_RCFD_series_2(
        start_date=START_DATE, end_date=END_DATE, wrds_username=WRDS_USERNAME
):
    """Pull RCFD series from WRDS.
    """
    sql_query = f"""
        SELECT 
            b.rssd9001, b.rssd9017, b.rssd9999, b.rcfda549,
            b.rcfda550, b.rcfda551, b.rcfda552, b.rcfda553, b.rcfda554, 
            b.rcfd2170
        FROM 
            bank.wrds_call_rcfd_2 AS b
        WHERE 
            b.rssd9999 BETWEEN '{start_date}' AND '{end_date}'
        """

    # Connect to the WRDS database
    db = wrds.Connection(wrds_username=wrds_username)
    
    # Execute the SQL query
    rcfd_series_2 = db.raw_sql(sql_query, date_cols=["rssd9999"])
    
    # Close the connection
    db.close()

    return rcfd_series_2


def load_RCON_series_1(data_dir=DATA_DIR):
    path = Path(DATA_DIR) / "RCON_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCON_series_2(data_dir=DATA_DIR):
    path = Path(DATA_DIR) / "RCON_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_1(data_dir=DATA_DIR):
    path = Path(DATA_DIR) / "RCFD_Series_1.parquet"
    comp = pd.read_parquet(path)
    return comp

def load_RCFD_series_2(data_dir=DATA_DIR):
    path = Path(DATA_DIR) / "RCFD_Series_2.parquet"
    comp = pd.read_parquet(path)
    return comp


def _demo():
    rcon_series_1 = load_RCON_series_1(data_dir=DATA_DIR)
    rcon_series_2 = load_RCON_series_2(data_dir=DATA_DIR)
    rcfd_series_1 = load_RCFD_series_1(data_dir=DATA_DIR)
    rcfd_series_2 = load_RCFD_series_2(data_dir=DATA_DIR)

if __name__ == "__main__":
    rcon_series_1  = pull_RCON_series_1(wrds_username=WRDS_USERNAME)
    path = Path(DATA_DIR) / "RCON_Series_1.parquet"
    rcon_series_1.to_parquet(path)

    rcon_series_2 = pull_RCON_series_2(wrds_username=WRDS_USERNAME)
    path = Path(DATA_DIR) / "RCON_Series_2.parquet"
    rcon_series_2.to_parquet(path)

    rcfd_series_1 = pull_RCFD_series_1(wrds_username=WRDS_USERNAME)
    path = Path(DATA_DIR) / "RCFD_Series_1.parquet"
    rcfd_series_1.to_parquet(path)

    rcfd_series_2 = pull_RCFD_series_2(wrds_username=WRDS_USERNAME)
    path = Path(DATA_DIR) / "RCFD_Series_2.parquet"
    rcfd_series_2.to_parquet(path)
