import pandas as pd
import numpy as np
from fbprophet import Prophet
from pymongo import MongoClient
import pymongo
import time
import api_keys

api = api_keys.api_keys()

client = pymongo.MongoClient(api.mongo)
# specify the database and collection`
db = client.gdax.gdaxws

def FBP(db,time_delta=3600*24):  #default value is 5 days
    df = pd.DataFrame(
        list(db.find({'MONGOKEY':'MARKET_UPDATE',
                             'product_id':'BTC-USD',
                             'timestamp' : {'$gt':time.time()-time_delta}})\
                        .sort([('timestamp', 1)]))) #sort from ascending

    df['time'] = pd.to_datetime(df['time'],infer_datetime_format=True)    
    df.set_index('time',drop=True, inplace=True)
    df = df.resample('min').mean()
    df['ds'] = df.index.copy(deep=True)

    m = Prophet(changepoint_prior_scale=0.0002).fit(df)
    future = m.make_future_dataframe(periods=1, freq='1Min')
    fcst_0002 = m.predict(future)
    fcst_0002 = fcst_0002.rename(columns=lambda x: x + "_fcst_0002")

    m = Prophet(changepoint_prior_scale=0.002).fit(df)
    future = m.make_future_dataframe(periods=1, freq='1Min')
    fcst_002 = m.predict(future)
    fcst_002 = fcst_002.rename(columns=lambda x: x + "_fcst_002")

    keep_columns = ['ds','yhat_lower','yhat_upper','yhat','trend']
    
    y_hats_0002 = fcst_0002.iloc[-1][[col + "_fcst_0002" for col in keep_columns]].to_dict()
    y_hats_002 = fcst_002.iloc[-1][[col + "_fcst_002" for col in keep_columns]].to_dict()
    return {**y_hats_0002,**y_hats_002}

def push_mongo(db, y_hats):
    y_hats.update({'MONGOKEY':'FBP_UPDATE','timestamp':time.time()})
    print(y_hats)
    db.btcusd.insert_one(y_hats)

if __name__ == "__main__":
    y_hats = FBP(db)
    push_mongo(db, y_hats)
    timer = time.time()
    
    while True:
        if time.time() - timer > 30:
            try:
                y_hats = FBP(db)
                push_mongo(db, y_hats)
                timer = time.time()
            except:
                time.sleep(4)
                timer = time.time()
                