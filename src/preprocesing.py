import pandas as pd

def preprocessing(data):
    """
    Perform preprocessing on financial data.

    Parameters:
    data (pd.DataFrame): Input financial data with a 'close' column.

    Returns:
    pd.DataFrame: Preprocessed financial data.
    """
    # Calculate daily percentage change
    data = data.pct_change()

    # Calculate 20-day rolling standard deviation (volatility)
    data['vol_20D'] = data['close'].rolling(20).std()

    return data

# Example Usage:
# Assuming you have a DataFrame called 'financial_data'
# preprocessed_data = preprocessing(financial_data)
# print(preprocessed_data)
