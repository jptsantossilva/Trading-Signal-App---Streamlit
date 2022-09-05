import pandas as pd
import websocket, json

from sqlalchemy import create_engine

engine = create_engine('sqlite:///CryptoDB.db')

stream = "wss://stream.binance.com:9443/ws/!miniTicker@arr"

def on_message(ws, message):
    msg = json.loads(message)
    # get only assets with USDT pair
    symbol = [x for x in msg if x['s'].endswith('USDT')]
    # filter dataframe with timestamp E, Symbol s and close price c
    frame = pd.DataFrame(symbol)[['E','s','c']]
    # make timestamp human readable
    frame.E = pd.to_datetime(frame.E, unit='ms')
    # convert close price to float
    frame.c = frame.c.astype(float)
    # loop dataframe and insert into database
    for row in range(len(frame)):
        data = frame[row:row+1]
        data[['E','C']].to_sql(data['s'].values[0], engine, index=False, if_exists='append')

# start websocket
ws = websocket.WebSocketApp(stream, on_message=on_message)
ws.run_forever()



