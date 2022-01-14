import pandas as pd
import requests as re
import pprint as pp
from datetime import date, datetime
import os
env_var = os.environ

token = os.environ["TWITTER_TOKEN"]

headers = {
        "Authorization" : "Bearer " + token
    }

def pull_tweet_counts(query, start_time = None, end_time = None, granularity = 'day'):
    # documentation https://developer.twitter.com/en/docs/twitter-api/tweets/counts/api-reference/get-tweets-counts-all
    # We need to get academic research access to get entire history as well as a larger api limit

    if start_time is None:
        base_url = f'https://api.twitter.com/2/tweets/counts/recent?query={query}&granularity={granularity}'
        response = re.get(base_url, headers = headers)
        return pd.DataFrame(response.json()['data'])


#pulls raw tweets and saves to pickle file
def pull_raw_tweets(query, start_time = None, end_time = None ):
    #Uses standard search API. 
    #documentation https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent
    if start_time is None: 
        base_url = f'https://api.twitter.com/2/tweets/search/recent/?query={query}&tweet.fields=created_at&expansions=author_id&user.fields=description&place.fields=country' 
        response = re.get(base_url, headers = headers)
        j = response.json()
        df = pd.DataFrame(j['data'])
        df.to_pickle(f'{query}_data.pkl')


def get_tweets(stock_ticker, date, max_results=10):
    # get tweets up to max_results for stock_ticker on a specific date
    base_url = f'https://api.twitter.com/2/tweets/search/all?query={stock_ticker}&start_time={date}T00:00:00.000Z&end_time={date}T23:59:59.999Z&max_results={max_results}&tweet.fields=created_at&expansions=author_id'
    response = re.get(base_url, headers = headers)
    j = response.json()
    df = pd.DataFrame(j['data'])
    return df


def write_tweets(stock_ticker, date, file_path, max_results=10):
    df = get_tweets(stock_ticker, date, max_results)
    df.to_pickle(file_path)

if __name__ == '__main__':
    pull_raw_tweets('TSLA')
    df = pd.read_pickle('TSLA_data.pkl')
    print(df)