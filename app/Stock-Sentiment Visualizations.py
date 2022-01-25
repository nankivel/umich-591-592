#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import altair as alt
import numpy as np

# for this project purpose we have only considered top FIVE USA based EV manufactures.
stickers = ['FSR','LCID','NKLA','RIVN','TSLA']

# this path has to be github app/data/processed path but for now using local
path = "//U-Mich/SIADS - 591-592 Milestone I/"


df_twitter = pd.DataFrame()
df_stock = pd.DataFrame()

for sticker in stickers:
    df_twitter_temp = pd.read_csv(path + sticker + "_twitter.csv")
    df_twitter_temp["sticker"] = sticker
    
    df_stock_temp = pd.read_csv(path + sticker + "_stock.csv")
    df_stock_temp["sticker"] = sticker
 
    if ((len(df_twitter) == 0) or (len(df_stock) == 0)):
        df_twitter = df_twitter_temp.copy()
        df_stock = df_stock_temp.copy()
        
    else:
        df_twitter = df_twitter.append(df_twitter_temp, ignore_index = True)
        df_stock = df_stock.append(df_stock_temp, ignore_index = True)

#df_twitter.head()
#df_stock.head()


# In[2]:


alt.Chart(df_stock).mark_line().encode(
    x='datetime',
    y= alt.Y('Close:Q', axis=alt.Axis(orient='right')),
    #y= alt.Y('Close:Q', scale=alt.Scale(type='log', base=2), axis=alt.Axis(orient='right')),
    color='sticker',
    strokeDash='sticker',
)

area = alt.Chart(df_stock).mark_area().encode(
    alt.X('datetime:O',  axis = None),
    alt.Y('Close:Q',axis=alt.Axis(title='Stock Price (USD) & Volume')),
    color="sticker:N"
).properties (height = 200,width = 500, title = "2021 Q3 Onwards - USA EV Stock")

area


# In[4]:


df_t = df_twitter
df_t['Year-Month'] = df_t['created_at'].str[:7]

df1 = df_t.groupby(['sticker','Year-Month'])['count'].sum().reset_index()

sentiment = alt.Chart(df1).mark_bar().encode(
    x=alt.X('sum(count)',stack="normalize", axis=alt.Axis(format='%', title='Tweet volume')),
    y='Year-Month',
    color='sticker'
).properties(height = 150, width = 500, title = "Talk of the town - USA top 5 EV makers")

sentiment


# In[5]:


# change the sticker 
param_sticker = 'RIVN'


#df_t = df_twitter[df_twitter["sticker"] == param_sticker]
df_t = df_twitter

df_t["date"] = df_t['created_at'].str[:10]
df_t["hour"] = df_t['created_at'].str[-2:]

#df_s = df_stock[df_stock["sticker"] == param_sticker]
df_s = df_stock


df_s["date"] = df_s['datetime'].str[:10]
df_s["hour"] = df_s['datetime'].str[-2:]

def calculate_sentiment(row):
    
    if ((row["positive"] >= row["negative"])) :
        sentiment = "POSITIVE"
    else : 
        sentiment = "NEGATIVE"
   
    return sentiment

df_t['sentiment'] = df_t.apply(calculate_sentiment, axis=1)

def calculate_sentiment_val(row):
    
    if ((row["positive"] >= row["negative"])) :
        sentiment_val = row["positive"]
    else : 
        sentiment_val = row["negative"]
   
    return sentiment_val

df_t['sentiment_val'] = df_t.apply(calculate_sentiment_val, axis=1)


def calculate_stock_trend(row):
    
    if ((row["Close"] >= row["Open"])) :
        stock_trend = "UP"
    else : 
        stock_trend = "DOWN"
   
    return stock_trend

df_s['stock_trend'] = df_s.apply(calculate_stock_trend, axis=1)


# In[7]:




df_t = df_t[df_twitter["sticker"] == param_sticker]
df_s = df_s[df_stock["sticker"] == param_sticker]


sentiment1 = alt.Chart(df_t).mark_circle(color = 'lightgreen').encode(
    x='date:O',
    y='hour:O',
    size='positive:O',
    #color=alt.Color('positive:O', scale=alt.Scale(domain=domain, range=range_))
    
).properties (height = 300,width = 800, title = param_sticker )

df2 = df_s[["date","hour","Close"]]

line = alt.Chart(df2).mark_line(color='#333').encode(
    x=alt.X('date:O', axis=alt.Axis(labels=True)),
    y=alt.Y('Close:Q', axis = None),
    color = 'hour:O'
    #y2 ='hour:O'
    #y='Close:Q',
    #y2 ='hour:O'
)

sentiment1 + line


# In[8]:




domain = ["POSITIVE","NEGATIVE","UP","DOWN"]
range_ = ['lightgreen','red',"green","red"]


sentiment_all = alt.Chart(df_t).mark_circle().encode(
    x='date:O',
    y='hour:O',
    size='sentiment_val:O',
    color=alt.Color('sentiment:O', scale=alt.Scale(domain=domain, range=range_))
    
).properties (height = 300,width = 800, title = param_sticker )

df2 = df_s[["date","hour","Close"]]

line = alt.Chart(df2).mark_line(color = 'black', size = 4).encode(
    x=alt.X('date:O', axis=alt.Axis(labels=True)),
    y=alt.Y('Close:Q', axis = None),
    #color = 'hour:O'

)

sentiment_all  + line


# In[9]:


stock = alt.Chart(df_s).mark_point(filled=True).encode(
    x='date:O',
    y='hour:O',
    #size='sentiment_val:O',
    color=alt.Color('stock_trend:O', scale=alt.Scale(domain=domain, range=range_))
    
).properties(height = 300)

sentiment_all + stock


# In[ ]:




