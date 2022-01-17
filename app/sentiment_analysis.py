import pandas as pd
import vaderSentiment
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import constants
import datetime as dt

def scrub_data(df):
    #Remove twitter handles- we dont want to do sentiment analysis on these
    df['text'] = df['text'].str.replace('@[^\s]+', "", regex = True)
    #Scrub retweet tag
    df['text'] = df['text'].str.replace('RT ', "", regex = True)
    #Scrub urls (basic way is to scrup string starting with https://)
    df['text'] = df['text'].str.replace('https://[a-zA-Z1-9\./]*', "", regex = True)
    #scrub newlines
    df['text'] = df['text'].str.replace('\n', "", regex = True)
    return df


def conduct_sentiment_analysis(df):
    analyzer = SentimentIntensityAnalyzer()
    df['sentiment'] = df['text'].apply(lambda x: analyzer.polarity_scores(x))
    df['positive'] = df['sentiment'].apply(lambda x: x['pos'])
    df['negative'] = df['sentiment'].apply(lambda x: x['neg'])
    df['neutral'] = df['sentiment'].apply(lambda x: x['neu'])
    return df

def sentiment_by_hour(df):
    #df['created_at'] = df['created_at'].dt.minute
    df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H')
    df = df.groupby(['created_at']).agg({'positive' : 'mean', 'neutral': 'mean', 'negative' : 'mean', 'id' : 'nunique'})
    df.rename(columns = {'id' : 'count'}, inplace = True)
    df.reset_index(inplace = True)
    return df

def sentiment_by_pkl(pickle_filepath):
    #merge above 3 functions and conduct sentiment analysis
    df = pd.read_pickle(pickle_filepath)
    df = scrub_data(df)
    df = conduct_sentiment_analysis(df)
    df = sentiment_by_hour(df)
    return df
    
def ticker_full_sentiment(ticker):
    li = []
    delta = dt.timedelta(days = 1)
    start_date = constants.start_date
    while start_date <= constants.end_date:
        path = f'data/raw/twitter/{ticker}_{start_date}.pkl'
        try:
            df_temp = sentiment_by_pkl(path)
            li.append(df_temp)
        except:
            print(f'Error: {path}')
        start_date += delta
    fin_df = pd.concat(li)
    fin_df.to_pickle(f'data/processed/twitter/{ticker}.pkl')

def main():
    for ticker in constants.list_stocks:
        ticker_full_sentiment(ticker)

if __name__ == '__main__':
    main()

    
