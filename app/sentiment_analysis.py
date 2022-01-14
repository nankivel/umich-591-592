import pandas as pd
import vaderSentiment
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def scrub_data(pickle_filepath):
    df = pd.read_pickle(pickle_filepath)
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
    df['compound'] = df['sentiment'].apply(lambda x: x['compound'])
    return df

def sentiment_by_minute(df):
    #df['created_at'] = df['created_at'].dt.minute
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['hour'] = df['created_at'].dt.hour
    df['minute'] = df['created_at'].dt.minute
    df['date'] = df['created_at'].dt.date
    df = df.groupby(['date', 'hour', 'minute']).mean()
    return df


if __name__ == '__main__':
    df = scrub_data('TSLA_data.pkl')
    df = conduct_sentiment_analysis(df)
    df = sentiment_by_minute(df)
    print(df)

    
