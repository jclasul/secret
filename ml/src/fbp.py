import pandas as pd
import numpy as np
from fbprophet import Prophet
from pymongo import MongoClient
import pymongo
import time
import apies

api = apies.api()

client = pymongo.MongoClient(api.mongo)
# specify the database and collection`
db = client.test
counter = 0

def FBP(db):
    df = pd.DataFrame(list(db.btcusd.find({'MONGOKEY' : 'MARKET_UPDATE'})\
        .sort([('timestamp', 1)]).limit(20000)))
    df['time'] = pd.to_datetime(df['time'],infer_datetime_format=True)
    df.rename(columns={'y':'y','time':'ds'}, inplace=True)

    m = Prophet(changepoint_prior_scale=0.0002).fit(df)
    future = m.make_future_dataframe(periods=1, freq='1Min')
    fcst = m.predict(future)

    y_hats = fcst.iloc[-1][['ds','yhat_lower','yhat_upper','yhat','trend']].to_dict()
    return y_hats

def push_mongo(db, y_hats):
    y_hats.update({'MONGOKEY' : 'FBP_UPDATE'})
    db.btcusd.insert_one(y_hats)

if __name__ == "__main__":
    y_hats = FBP(db)
    push_mongo(db, y_hats)
    timer = time.time()
    
    with client.test.btcusd.watch() as stream:
        if time.time() - timer > 30:
            y_hats = FBP(db)
            push_mongo(db, y_hats)
            counter = 0
            timer = time.time()

        for change in stream:
            print(change)
            MONGOKEY = change.get('fullDocument').get('MONGOKEY', None)

            if MONGOKEY != 'FBP_UPDATE':
                print()
                counter += 1
                print(counter)

            if counter >= 5:
                y_hats = FBP(db)
                push_mongo(db, y_hats)
                counter = 0

                
    