import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np

from sqlalchemy import create_engine
engine = create_engine('sqlite:///CryptoDB.db')

symbols = pd.read_sql('SELECT name FROM sqlite_master WHERE type="table"',
                        engine).name.to_list()

st.title('Live TA platform')

def applytechnicals(df):
    df['SMA_fast'] = df.c.rolling(7).mean()
    df['SMA_slow'] = df.c.rolling(25).mean()
    df.dropna(inplace=True)

def qry(symbol):
    now = dt.datetime.utcnow()
    before = now - dt.timedelta(minutes=30)
    qry_str = f"""SELECT E,c FROM '{symbol}' WHERE E >= '{before}'"""
    df = pd.read_sql(qry_str,engine)
    # convert timestamp from string to datetime
    df.E = pd.to_datetime(df.E)
    #resample the data to get the last price of the minute
    df = df.set_index('E')
    df = df.resample('1min').last()
    applytechnicals(df)
    # set position = 1 where fast SMA > slow SMA
    df['position'] = np.where(df['SMA_fast'] > df['SMA_slow'], 1, 0)
    return df

def check():
    for symbol in symbols:
        if len(qry(symbol).position) > 1:
            if qry(symbol).position[-1] and qry(symbol).position.diff()[-1]:
                st.write(symbol)

st.button('Get Live MA cross', on_click=check())





