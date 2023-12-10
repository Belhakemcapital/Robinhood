
import numpy as np
from pandas import DataFrame
from typing import List, Dict, Union

def verify_data_is_not_empty(df: DataFrame) -> None:
    assert not df.empty, "Dataframe is empty"

def verify_dataframe_is_not_all_nan(df: DataFrame) -> None:
    assert not df.isnull().all().all(), "Entire DataFrame is NaN"

def verify_last_column_is_nan(df: DataFrame) -> None:
    last_column = df.columns[-1]
    assert df[last_column].isnull().all(), f"Last column {last_column} is not all NaN"

def verify_before_last_column_is_not_nan(df: DataFrame) -> None:
    before_last_column = df.columns[-2]
    assert not df[before_last_column].isnull().any(), f"Second to last column {before_last_column} contains NaN values"

def verify_no_duplicates(df: DataFrame) -> None:
    assert not df.duplicated().any(), "Dataframe contains duplicate rows"

def verify_column_names(df: DataFrame, column_names: List[str]) -> None:
    assert set(df.columns) == set(column_names), "Column names are not as expected"

def verify_data_types(df: DataFrame, column_types: Dict[str, Union[str, type]]) -> None:
    for column, type_ in column_types.items():
        assert df[column].dtype == np.dtype(type_), f"{column} is not of type {type_}"

def verify_dates_are_sorted(df: DataFrame, date_column: str) -> None:
    assert (df[date_column].sort_values().reset_index(drop=True) == df[date_column].reset_index(drop=True)).all(), "Dates are not sorted"

def verify_data_range(df: DataFrame, column: str, min_value: Union[int, float], max_value: Union[int, float]) -> None:
    assert df[column].between(min_value, max_value).all(), f"{column} contains values out of range"

def verify_are_columns_are_float(df: DataFrame, columns: List[str]) -> None:
    for column in columns:
        assert df[column].dtype in ['float16', 'float32', 'float64'], f"Column {column} is not of type float"

