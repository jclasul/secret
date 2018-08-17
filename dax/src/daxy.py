from pymongo import MongoClient
import time
import api_keys
import requests
import gdax
import json

api = api_keys.api_keys()

client = MongoClient(api.mongo)
# specify the database and collection`
db = client.gdax.gdaxws
print('connected to mongo collection')

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
    
    def pingexchange(self):
        pass

if __name__ == "__main__":
    client = GAC(key=key,b64secret=b64secret,
                 passphrase=passphrase,
                 api_url="https://api.pro.coinbase.com")
