import yfinance as yf
import pandas as pd
import constants
import datetime as dt
import numpy as np

def pull_data(ticker):
    df = yf.download(tickers=ticker, period='1y', interval='1h', start = constants.start_date)
    df.to_pickle(f'data/raw/stocks/{ticker}.pkl')

def calculate_hourly_returns(df):
    df.reset_index(inplace = True)
    df.rename(columns = {'index': 'datetime'}, inplace = True)
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H')
    df['pct_change'] = df['Adj Close'].pct_change()
    df['log_return'] = np.log(1 + df['pct_change'])
    return df

def main():
    for ticker in constants.list_stocks:
        in_path = f'data/raw/stocks/{ticker}.pkl'
        out_path = f'data/processed/stocks/{ticker}.pkl'
        df = pd.read_pickle(in_path)
        df = calculate_hourly_returns(df)
        df.to_pickle(out_path)


if __name__ == '__main__':
    main()
    