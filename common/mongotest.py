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
            print(change.get('fullDocument').get('MONGOKEY', None),
                             change.get('fullDocument').get('_id', None))