from nbformat import write
import pandas as pd
import requests as re
import pprint as pp
from datetime import date
import os
from pathlib import Path
import constants

env_var = os.environ
token = os.environ["TWITTER_TOKEN"]
headers = {"Authorization" : "Bearer " + token}


def get_tweets(stock_ticker, date, max_results=10):
    # get tweets up to max_results for stock_ticker on a specific date
    date = date.strftime("%Y-%m-%d")
    # base_url = f'https://api.twitter.com/2/tweets/search/all?query={stock_ticker}&start_time={date}T00:00:00.000Z&end_time={date}T23:59:59.999Z&max_results={max_results}&tweet.fields=created_at&expansions=author_id'
    base_url = f'https://api.twitter.com/2/tweets/search/all'
    query = f'?query={stock_ticker}'
    start_time = f'&start_time={date}T00:00:00.000Z'
    end_time = f'&end_time={date}T23:59:59.999Z'
    max_results_param = f'&max_results={max_results}'
    fields = '&tweet.fields=created_at&expansions=author_id'
    if max_results is not None:
        request = f'{base_url}{query}{start_time}{end_time}{max_results_param}{fields}'
    else:
        request = f'{base_url}{query}{start_time}{end_time}{fields}'
    response = re.get(request, headers = headers)
    j = response.json()
    df = pd.DataFrame(j['data'])
    return df


def write_stock_daily_tweets(stock_ticker, date, file_path, max_results=10):
    df = get_tweets(stock_ticker, date, max_results)
    df.to_pickle(file_path)


def list_business_days(start_date, end_date=date.today()):
    start_date = pd.to_datetime(start_date)
    datelist = pd.bdate_range(start_date, periods=1000).tolist()
    return [x for x in datelist if x < pd.Timestamp(end_date)]


def pull_tweet_counts(query, granularity = 'day'):
    base_url = f'https://api.twitter.com/2/tweets/counts/all?query={query}&granularity={granularity}'
    response = re.get(base_url, headers = headers)
    return pd.DataFrame(response.json()['data'])


if __name__ == '__main__':
    # TODO: figure out why I'm still only getting 10 tweets, maybe I need to specify max results. Could use the counts function to get an accurate max_results value
    list_days = list_business_days(start_date='2021-10-01')
    for s in constants.list_stocks:
        for d in list_days:
            file_path = f'~/Downloads/{s}_{d.year}-{str(d.month).zfill(2)}-{str(d.day).zfill(2)}.pkl'
            file_test = Path(file_path).expanduser()
            if not file_test.is_file():
                write_stock_daily_tweets(stock_ticker=s, 
                date=d, 
                file_path=file_path,
                max_results=None
                )
