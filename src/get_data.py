from os import environ
import sys
import pandas as pd
import numpy as np
import logging
from datetime import date, datetime, timedelta
from coinmetrics.api_client import CoinMetricsClient
import json
import logging
from typing import List, Dict
from os import environ
import pandas as pd
from coinmetrics.api.client import CoinMetricsClient
from datetime import datetime, timedelta
from typing import List
import pandas as pd

def get_metrics_names() -> List[str]:
    """
    Read metric names from a file and return them as a list.

    Returns:
    List[str]: A list containing metric names.

    Raises:
    FileNotFoundError: If the specified file is not found.
    Exception: If an error occurs while reading the file.
    """
    # Specify the file path
    file_path = 'input_data/metrics.txt'

    # Initialize an empty list to store metric names
    metric_names = []

    try:
        # Open the file in read mode
        with open(file_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Remove leading and trailing whitespaces and add each line to the list
            metric_names = [line.strip() for line in lines]

    except FileNotFoundError:
        # Handle file not found error
        print(f"File '{file_path}' not found.")
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred: {e}")

    # Return the list of metric names
    return metric_names


def get_asset_names()-> List[str]:
     """
    Read metric names from a file and return them as a list.

    Returns:
    List[str]: A list containing asset names.

    Raises:
    FileNotFoundError: If the specified file is not found.
    Exception: If an error occurs while reading the file.
    """
    # Specify the file path
    file_path = 'input_data/metrics.txt'

    # Initialize an empty list to store asset names
    asset_names = []

    try:
        # Open the file in read mode
        with open(file_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Remove leading and trailing whitespaces and add each line to the list
            asset_names = [line.strip() for line in lines]

    except FileNotFoundError:
        # Handle file not found error
        print(f"File '{file_path}' not found.")
    except Exception as e:
        # Handle other exceptions
        print(f"An error occurred: {e}")

    # Return the list of asset names
    return asset_names




def configure_logger() -> None:
    """
    Configure the logging format and level.

    This function configures the logging format, level, and date format.

    Returns:
    None
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def initialize_coin_metrics_client(api_key: str = "") -> CoinMetricsClient:
    """
    Initialize the CoinMetricsClient with the provided API key.

    This function initializes the CoinMetricsClient using the provided API key.
    If no API key is provided, it uses an empty string.

    Args:
    api_key (str): The API key for accessing CoinMetrics services.

    Returns:
    CoinMetricsClient: An instance of the CoinMetricsClient.
    """
    # Use the API key from the environment if available
    try:
        api_key = environ["CM_API_KEY"]
        logging.info("Using API key found in environment")
    except KeyError:
        logging.info("API key not found. Using an empty string.")

    # Initialize the CoinMetricsClient
    client = CoinMetricsClient(api_key)

    return client

def fetch_asset_metrics(
    client: CoinMetricsClient,
    assets: List[str],
    metrics: List[str],
    start_time: str,
    frequency: str = '1d'
) -> pd.DataFrame:
    """
    Fetch asset metrics using the CoinMetricsClient.

    This function fetches asset metrics using the CoinMetricsClient for specified
    assets, metrics, start time, and frequency.

    Args:
    client (CoinMetricsClient): An instance of the CoinMetricsClient.
    assets (List[str]): A list of asset symbols.
    metrics (List[str]): A list of metric names.
    start_time (str): The start time for fetching metrics data.
    frequency (str): The frequency of the data (default is '1d').

    Returns:
    pd.DataFrame: A pandas DataFrame containing the fetched asset metrics.
    """
    # Fetch asset metrics
    metrics_data = client.get_asset_metrics(
        assets=assets,
        start_time=start_time,
        metrics=metrics,
        frequency=frequency
    ).to_dataframe()

    return metrics_data




def get_coinmetrics_data(days_before_today: int = 3) -> pd.DataFrame:
    """
    Fetch asset metrics data from CoinMetrics.

    This function configures the logger, initializes the CoinMetricsClient,
    retrieves asset names and metrics, calculates the start time (3 days before today),
    and fetches asset metrics data using the CoinMetricsClient.

    Args:
    days_before_today (int): Number of days before today for the start time (default is 3).

    Returns:
    pd.DataFrame: A pandas DataFrame containing the fetched asset metrics data.
    """
    # Configure logger and initialize CoinMetricsClient
    configure_logger()
    coin_metrics_client = initialize_coin_metrics_client(api_key)

    # Retrieve asset names and metrics
    assets = get_asset_names()
    metrics = get_asset_metrics()

    # Calculate the start time (3 days before today)
    start_time = (datetime.now() - timedelta(days=days_before_today)).strftime('%Y-%m-%d')

    # Set the data fetching frequency
    frequency = '1d'

    # Fetch asset metrics data
    metrics_data = fetch_asset_metrics(coin_metrics_client, assets, metrics, start_time, frequency)

    return metrics_data

