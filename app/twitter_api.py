import pandas as pd
import requests as re
import json
import pprint as pp

api_key = 'gYRKFJvCkWkvgAADVRtPMzO3X'
api_key_secret = 'p4tFlh6n7R8n3NtjNL8F1o3GOgHF4zSrogbZOFbzeVINCbD5XD'
token = 'AAAAAAAAAAAAAAAAAAAAAExZXwEAAAAAlPNNzcxPq%2F1%2FYewFwvRl49RYwdA%3Dg9nPclqkeynaGHyJdWGNyg3cr2AIfDD7epEzdwPWv0GdLDGWso'

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



if __name__ == '__main__':
    pull_raw_tweets('TSLA')
    df = pd.read_pickle('TSLA_data.pkl')
    print(df)