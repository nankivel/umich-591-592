import pandas as pd
import requests as re
import pickle
import pprint as pp

api_key = 'hrqN7ZrvkrkwcTiAEaU6XOLu3'
api_key_secret = 'AwbB0pvmCQKS1zMFp5CqLhwwr8ZuoBsGzv0izlLGoRofL6zr7M'
token = 'AAAAAAAAAAAAAAAAAAAAADxkXQEAAAAARGzvpgoEIvUgz1gq08zoGdVy%2F74%3DmcQSMELSeqYeivxEhi5WfO49xoCNo1YKEoSxbQaOI3385mY9Sp'

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


def pull_raw_tweets(query, start_time = None, end_time = None ):
    #Uses standard search API. 
    #documentation https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent
    if start_time is None: 
        base_url = f'https://api.twitter.com/2/tweets/search/recent/?query={query}&tweet.fields=public_metrics&expansions=author_id&user.fields=description' 
        response = re.get(base_url, headers = headers)
        return response.json()


if __name__ == '__main__':
    print(pull_tweet_counts('lithium ion'))
    pp.pprint(pull_raw_tweets('lithium ion'))
