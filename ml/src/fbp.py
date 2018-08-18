import pandas as pd
import numpy as np
from fbprophet import Prophet
from pymongo import MongoClient
from bson.objectid import ObjectId
import random
import pymongo
import time
import api_keys

api = api_keys.api_keys()

client = pymongo.MongoClient(api.mongo)
# specify the database and collection`
db = client.gdax.gdaxws

class ML():
    def FBP(self, db,time_delta=3600*24*2):  #default value is 5 days
        random_ = random.random()
        if random_ > 0.5:
            self.product_id = 'BTC-USD'
        else:
            self.product_id = 'ETH-USD'

        print('product_id {} : {}'.format(self.product_id, random_))
        df = pd.DataFrame(
            list(db.find({'MONGOKEY':'MARKET_UPDATE',
                                'product_id':self.product_id,
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

    def push_mongo(self, db, y_hats, current_time):
        y_hats.update({'MONGOKEY':'FBP_UPDATE',
                        'timestamp':current_time,
                        '_id':','.join([str(y_hats['ds_fcst_0002']),self.product_id]),
                        'product_id':self.product_id})
        
        try:
            db.insert_one(y_hats)
            print(y_hats)
        except Exception:
            print(time.ctime(),'catching exception parsing mongo')

if __name__ == "__main__":
    ML = ML()
    timer = time.time()
    random_interval = random.randint(10,40)

    FBP = ML.FBP
    push_mongo = ML.push_mongo

    random_interval = 5 # quickstart    
    print('*** FBP starting')
    while True:
        current_time = time.time()
        if current_time - timer > random_interval:
            try:
                y_hats = FBP(db)
                push_mongo(db, y_hats, current_time)
                timer = time.time()
                random_interval = random.randint(10,40)
            except:
                time.sleep(4)
                timer = time.time()
                random_interval = random.randint(10,40)
                