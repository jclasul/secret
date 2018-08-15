from pymongo import MongoClient
import requests
import time
import gdax
import apies
import json

api = apies.api()
mongo_client = MongoClient(api.mongo) #change for mongo

# specify the database and collection
db = mongo_client.test #change for mongo

# and store in local variables
key = api.api_key
b64secret = api.secret_key
passphrase = api.passphrase

class GAC(gdax.AuthenticatedClient):
    def buy(self, **kwargs):
        kwargs["side"] = "buy"
        r = requests.post(self.url + '/orders',
                          data=json.dumps(kwargs),
                          auth=self.auth,
                          timeout=30)

        print(r)
        return r.json()

    def sell(self, **kwargs):
        kwargs["side"] = "sell"
        r = requests.post(self.url + '/orders',
                          data=json.dumps(kwargs),
                          auth=self.auth,
                          timeout=30)

        print('SOLD !!')
        print(r)
        db.orders.insert_one(r.json())
        return r.json()

if __name__ == "__main__":
    client = GAC(key=key,b64secret=b64secret,\
                 passphrase=passphrase,\
                 api_url="https://api.pro.coinbase.com")

    client.sell(size="0.005000000",
                product_id="BTC-EUR",
                side="sell",
                stp="dc",
                type="market")
