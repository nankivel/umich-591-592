import yfinance as yf
import plotly.graph_objs as go

def plot_stock(stock_ticker):
    df = yf.download(tickers=stock_ticker, period='10y', interval='1d')
    print(len(df))
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df.index,
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name= 'market data'
        )
                 )

    fig.update_layout(
        title=stock_ticker,
        yaxis_title='Stock Price (USD per share)'
    )

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label='1M', step='month', stepmode='backward'),
                dict(count=1, label='YTD', step='year', stepmode='todate'),
                dict(count=1, label='1Y', step='year', stepmode='backward'),
                dict(count=3, label='3Y', step='year', stepmode='backward'),
                dict(count=5, label='5Y', step='year', stepmode='backward'),
                dict(step='all')
            ])
        )
    )

    return fig