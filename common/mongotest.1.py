from pymongo import MongoClient
import time
import api_keys
api = api_keys.api_keys()

client = MongoClient(api.mongo)
print(client)
# specify the database and collection`
db = client.gdax
print('connected to mongo collection')

if __name__ == "__main__":
    with db.gdaxws.watch() as stream:
        for change in stream:
            if change.get('fullDocument').get('MONGOKEY', None) == "FBP_UPDATE":
                keys = ['MONGOKEY',
                        'yhat_lower_fcst_002', 'yhat_lower_fcst_0002',
                        'yhat_upper_fcst_002', 'yhat_upper_fcst_0002']
                print(time.ctime(),[change.get('fullDocument').get(ckey,None) for ckey in keys])