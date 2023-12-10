import logging
import numpy as np
from pandas import (
    DataFrame,
    isna,
    to_datetime,
    date_range
)
from typing import List, Dict, Union

from src.get_data import get_asset_names

logging.basicConfig(level=logging.ERROR)

def verify_data_is_not_empty(df: DataFrame, asset_name: str) -> None:
    logging.info(f"Verifying {asset_name} data is not empty")
    try:
        assert not df.empty
    except:
        print(f"AssertionError: {asset_name}: Dataframe is empty")

def verify_dataframe_is_not_all_nan(
    df: DataFrame,
    asset_name: str,
    not_nan_columns_names: List[str] = ['asset',"time", "ReferenceRate", "ReferenceRateUSD", "ReferenceRateEUR"],
) -> None:
    logging.info(f"Verifying {asset_name} dataframe is not all NaN")
    try:
        assert not isna(df.drop(columns=not_nan_columns_names)).all().all()
    except:
        print(f"AssertionError: {asset_name}: Entire DataFrame is not NaN")

def verify_last_row_is_nan(
    df: DataFrame,
    asset_name: str,
    not_nan_columns_names: List[str] = ['asset',"time", "ReferenceRate", "ReferenceRateUSD", "ReferenceRateEUR"]
) -> None:
    logging.info(f"Verifying {asset_name} last row is all NaN")
    try:
        assert isna(df.drop(columns=not_nan_columns_names).iloc[-1]).all()
    except:
        print(f"AssertionError: {asset_name}: Last row is not all NaN")

def verify_second_to_last_row_is_not_nan(
    df: DataFrame,
    asset_name: str,
    not_nan_columns_names: List[str] = ['asset',"time", "ReferenceRate", "ReferenceRateUSD", "ReferenceRateEUR"]
) -> None:
    logging.info(f"Verifying {asset_name} second to last row is not all NaN")
    try:
        not isna(df.drop(columns=not_nan_columns_names).iloc[-2]).all(), f"Second to last row is not all NaN"
    except:
        print(f"AssertionError: {asset_name}: Second to last row is all NaN")

def verify_no_duplicates(df: DataFrame, asset_name: str,) -> None:
    logging.info(f"Verifying {asset_name} dataframe has no duplicates")
    try:
        assert not df.duplicated().any()
    except:
        print(f"AssertionError: {asset_name}: DataFrame contains duplicates")

def verify_column_names(
    df: DataFrame,
    asset_name: str,
    file_path_metrics: str = '../data/static/metrics.txt',
    default_columns: List[str] = ['asset',"time"]
) -> None:
    logging.info(f"Verifying {asset_name} column names are as expected")
    metrics_names = get_asset_names(file_path_metrics)
    try:
        assert set(df.columns) == set(metrics_names+default_columns)
    except AssertionError:
        logging.error(f"AssertionError: {asset_name}: Column names are not as expected")
        print(f"Columns not downloaded: {asset_name}: ", set(metrics_names+default_columns).difference(set(df.columns)))
        print(f"Columns downloaded but not in wanted list {asset_name}: ", set(df.columns).difference(set(metrics_names+default_columns)))

def verify_data_types(
    df: DataFrame,
    asset_name: str,
    file_path_metrics: str = '../data/static/metrics.txt',
) -> None:
    logging.info(f"Verifying {asset_name} data types are as expected")
    metrics_names = get_asset_names(file_path_metrics)
    df_columns = df.columns
    wanted_downloaded_columns = set(metrics_names).intersection(set(df_columns))
    allowed_types = ['Int64', 'Float64']
    for col in wanted_downloaded_columns:
        try:
            assert df[col].dtype in allowed_types
        except AssertionError:
            print(f"AssertionError: {asset_name}: {col} is not of type {allowed_types}. it is of type {df[col].dtype}")

def verify_dates_are_sorted(df: DataFrame, date_column: str, asset_name: str,) -> None:
    logging.info(f"Verifying {asset_name} dates are sorted")
    try:
        assert (df[date_column].sort_values().reset_index(drop=True) == df[date_column].reset_index(drop=True)).all()
    except:
        logging.error(f"AssertionError: {asset_name}: Dates are not sorted")

def verify_no_missing_dates(df: DataFrame, asset_name: str) -> None:
    logging.info(f"Verifying {asset_name} has no missing dates")
    df['time'] = to_datetime(df['time']).dt.date
    possible_date_range = date_range(start=df['time'].min(), end=df['time'].max()).date
    try:
        assert set(possible_date_range).issubset(df['time'])
    except:
        print(f"AssertionError: {asset_name}: Missing dates {set(possible_date_range).difference(df['time'])}")

def verify_values_positive(
    df: DataFrame,
    asset_name: str,
    file_path_metrics: str = '../data/static/metrics.txt',
) -> None:
    logging.info(f"Verifying {asset_name} values are positive")
    metrics_names = get_asset_names(file_path_metrics)
    df_columns = df.columns
    wanted_downloaded_columns = list(set(metrics_names).intersection(set(df_columns)))
    df_with_wanted_downloaded_columns = df.loc[:,wanted_downloaded_columns]
    df_with_wanted_downloaded_columns_no_na = df_with_wanted_downloaded_columns.dropna()
    try:
        assert (df_with_wanted_downloaded_columns_no_na.loc[:,wanted_downloaded_columns] >= 0).all().all()
    except:
        print(f"AssertionError: {asset_name}: DataFrame contains negative values : {df_with_wanted_downloaded_columns_no_na[df_with_wanted_downloaded_columns_no_na < 0]}")

def calculate_nan_percentage(df: DataFrame, asset_name: str,) -> None:
    logging.info(f"Calculating {asset_name} NaN percentage")
    logging.info(f"{asset_name}: The percentage of NaN values (except the last row) is: {df.iloc[:-1].isna().mean().mean() * 100:.2f}%")

# =============================================================================
def check_data_quality(coinmetrics_data: DataFrame) -> None:
    not_nan_columns_names = ['asset',"time", "ReferenceRate", "ReferenceRateUSD", "ReferenceRateEUR"]
    file_path_metrics = '../data/static/metrics.txt'
    default_columns = ['asset',"time"]
    date_column='time'
    for (asset_name, df) in coinmetrics_data.groupby('asset'):
        verify_data_is_not_empty(df, asset_name)
        verify_dataframe_is_not_all_nan(df, asset_name, not_nan_columns_names)
        verify_last_row_is_nan(df, asset_name, not_nan_columns_names)
        verify_second_to_last_row_is_not_nan(df, asset_name, not_nan_columns_names)
        verify_no_duplicates(df, asset_name)
        verify_column_names(df, asset_name, file_path_metrics, default_columns)
        verify_data_types(df, asset_name, file_path_metrics)
        verify_dates_are_sorted(df, date_column, asset_name)
        verify_no_missing_dates(df, asset_name)
        verify_values_positive(df, asset_name, file_path_metrics)
        calculate_nan_percentage(df, asset_name)

