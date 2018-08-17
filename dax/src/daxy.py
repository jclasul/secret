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
    
    def getbalance(self):
        """{'id': '459d001f-0391-4e97-89e7-ae474275e2c9', 'currency': 'BTC',
            'balance': '0.0530287336346057', 'available': '0.0530287336346057',
            'hold': '0.0000000000000000', 'profile_id': '5100622b-3ed2-49e4-9810-c28fb96d30b3'} """

        self.balances = client.get_accounts()
        self.balance_BTC = next(item for item in self.balances if item["currency"] == "BTC")
        self.balance_ETH = next(item for item in self.balances if item["currency"] == "ETH")
        self.balance_EUR = next(item for item in self.balances if item["currency"] == "EUR")
        print('EUR balance at {}'.format(self.balance_EUR.get('balance', None)))
        print('BTC balance at {}'.format(self.balance_BTC.get('balance', None)))

if __name__ == "__main__":
    client = GAC(key=key,b64secret=b64secret,
                 passphrase=passphrase,
                 api_url="https://api.pro.coinbase.com")
    
    client.getbalance()
    counter = 0
    p_change = {}

    with db.watch() as stream:
        for change in stream:
            if counter >= 3 and change.get('fullDocument').get('MONGOKEY', None) == "FBP_UPDATE" and \
                    change.get('fullDocument').get('product_id', None) == "BTC-USD":

                print('DAXY DEBUGGING: running limit loop')
                print('DAXY price at : {}'.format(p_change.get('fullDocument').get('y', None)))
                keys = ['_id',
                        'yhat_lower_fcst_0002', 'yhat_lower_fcst_002',
                        'yhat_upper_fcst_002', 'yhat_upper_fcst_0002']
                print(time.ctime(),[change.get('fullDocument').get(ckey,None) for ckey in keys])
                counter = 0

            if change.get('fullDocument').get('MONGOKEY', None) == "MARKET_UPDATE" and \
                    change.get('fullDocument').get('product_id', None) == "BTC-USD":

                counter += 1
                keys = ['_id','y']
                print('{0}DAXY price at : {1} - {2}'.\
                    format(time.ctime(),change.get('fullDocument').get('y',None), counter))

                p_change = change