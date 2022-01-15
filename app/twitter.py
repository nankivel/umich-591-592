import pandas as pd
import requests
from datetime import date
import time
import os
from pathlib import Path
import constants

# To set your environment variables in your terminal run the following line:
# export 'TWITTER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("TWITTER_TOKEN")

search_url = "https://api.twitter.com/2/tweets/search/all"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def connect_to_endpoint(url, params):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get_tweets(stock_ticker, date):
    date = date.strftime("%Y-%m-%d")
    query_params = {
        'query': stock_ticker,
        'start_time': f'{date}T00:00:00.000Z',
        'end_time': f'{date}T23:59:59.999Z',
        'tweet.fields': 'created_at,author_id,public_metrics,source',
        'max_results': 50 # range 10 to 50
    }

    j_initial = connect_to_endpoint(search_url, query_params)
    df = pd.DataFrame(j_initial['data'])
    next_token = j_initial['meta'].get('next_token')
    print(f'got initial {len(df)} records for {stock_ticker} on {date}')

    while next_token is not None:
        query_params['next_token'] = next_token
        time.sleep(1)
        j = connect_to_endpoint(search_url, query_params)
        df = df.append(pd.DataFrame(j['data']))
        print(f'got total {len(df)} records for {stock_ticker} on {date}')
        next_token = j['meta'].get('next_token')

    return df


def write_stock_daily_tweets(stock_ticker, date, file_path):
    df = get_tweets(stock_ticker, date)
    df.to_pickle(file_path)


def list_business_days(start_date, end_date=date.today()):
    start_date = pd.to_datetime(start_date)
    datelist = pd.bdate_range(start_date, periods=1000).tolist()
    return [x for x in datelist if x < pd.Timestamp(end_date)]


def main():
    list_days = list_business_days(start_date='2021-10-01')
    for s in constants.list_stocks:
        for d in list_days:
            file_path = f'~/Downloads/{s}_{d.year}-{str(d.month).zfill(2)}-{str(d.day).zfill(2)}.pkl'
            file_test = Path(file_path).expanduser()
            if file_test.is_file():
                print(f'{file_test} already exists.')
            else:
                write_stock_daily_tweets(stock_ticker=s, 
                date=d, 
                file_path=file_path
                )
                time.sleep(300)

if __name__ == '__main__':
    main()
