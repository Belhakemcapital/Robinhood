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
from typing import List
import pandas as pd
from binance.client import Client
import datetime

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
    coin_metrics_client = initialize_coin_metrics_client()

    # Retrieve asset names and metrics
    assets = get_asset_names()
    metrics = get_metrics_names()

    # Calculate the start time (3 days before today)
    start_time = (datetime.now() - timedelta(days=days_before_today)).strftime('%Y-%m-%d')

    # Set the data fetching frequency
    frequency = '1d'

    # Fetch asset metrics data
    metrics_data = fetch_asset_metrics(coin_metrics_client, assets, metrics, start_time, frequency)

    return metrics_data



def read_api_keys(file_path: str = 'ID/test.txt') -> tuple:
    """
    Read API keys from a text file.

    Parameters:
    file_path (str): The file path to the text file containing API keys.

    Returns:
    Tuple[str, str]: A tuple containing API key and API secret.
    """
    with open(file_path) as f:
        lines = f.readlines()

    api_key = lines[0][0:-1]
    api_secret = lines[1][0:-1]
    return api_key, api_secret

def get_trading_pairs(info: pd.DataFrame) -> List[str]:
    """
    Retrieves a list of trading pairs involving the BUSD (Binance USD) quote asset that are currently open for trading.

    Parameters:
    info (pd.DataFrame): A DataFrame containing information about available trading pairs.

    Returns:
    List[str]: A list of trading pair symbols involving the BUSD quote asset that are currently open for trading.
    """
    ticker_usdt = []
    for c in info['symbols']:
        if c['quoteAsset'] == 'USDT' and c['status'] == "TRADING" and 'MARGIN' in c['permissions']:
            ticker_usdt.append(c['symbol'])
    return ticker_usdt

def fetch_candlestick_data(client: Client, symbol: str, interval: str, days_back: int = 2000) -> pd.DataFrame:
    """
    Fetch historical candlestick data for a given trading pair.

    Parameters:
    client (Client): An instance of the Binance Client.
    symbol (str): The trading pair symbol.
    interval (str): The candlestick interval (e.g., '1day', '1hour').
    days_back (int): Number of days back to fetch historical data (default is 2000).

    Returns:
    pd.DataFrame: A DataFrame containing historical candlestick data.
    """
    since_this_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
    until_this_date = datetime.datetime.now()
    candle = client.futures_historical_klines(symbol, interval, str(since_this_date), str(until_this_date))
    
    # Create a dataframe to label all the columns returned by Binance for later use
    columns_hist = ['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime',
                    'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore']
    
    df = pd.DataFrame(candle, columns=columns_hist)
    df['dateTime'] = pd.to_datetime(df['dateTime'], unit='ms')
    df['closeTime'] = pd.to_datetime(df['closeTime'], unit='ms')
    df['ticker'] = symbol
    
    # Convert data types for proper analysis
    df = df.astype({
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64',
        'quoteAssetVolume': 'float64',
        'numberOfTrades': 'int64',
        'takerBuyBaseVol': 'float64',
        'takerBuyQuoteVol': 'float64',
        'ticker': 'str'
    })
    df = df[['dateTime','ticker','close']]
    return df


def fetch_all_candlestick_data(client: Client, trading_pairs: List[str]) -> pd.DataFrame:
    """
    Fetch historical candlestick data for all trading pairs and concatenate the results.

    Parameters:
    client (Client): An instance of the Binance Client.
    trading_pairs (List[str]): A list of trading pair symbols.

    Returns:
    pd.DataFrame: A DataFrame containing concatenated historical candlestick data for all trading pairs.
    """
    all_data = pd.DataFrame()
    
    for symbol in trading_pairs:
        candlestick_data = fetch_candlestick_data(client, symbol, Client.KLINE_INTERVAL_1DAY)
        all_data = pd.concat([all_data, candlestick_data], ignore_index=True)
    
    return all_data


def main():
    # Read API keys from the file
    api_key, api_secret = read_api_keys()

    # Initialize Binance client
    client = Client(api_key, api_secret)

    # Get trading pairs information
    info = pd.DataFrame.from_dict(client.get_exchange_info()['symbols']).set_index('symbol')
    trading_pairs = get_trading_pairs(info)
    
    coinmetrics_data = get_coinmetrics_data().rename(columns={'time':'dateTime','asset':'ticker'})
    binance_data = fetch_all_candlestick_data(client, trading_pairs)
    all_data = coinmetrics_data.merge(binance_data, how='inner',on=['dateTime','ticker']).set_index(['dateTime','ticker'])
    all_data.to_parquet('./data/all_data.parquet')
    return all_data
