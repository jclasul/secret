from pymongo import MongoClient
import time
import api_keys
import requests
import gdax
import json
import pandas as pd

api = api_keys.api_keys()

client = MongoClient(api.mongo)
# specify the database and collection`
db = client.gdax.gdaxws
print("connected to mongo collection")

# and store in local variables
key = api.api_key
b64secret = api.secret_key
passphrase = api.passphrase

class clearingmaster():
    def __init__(self, maxopenbuyorders = 4 ,**kwargs):
        self.maxopenbuyorders = maxopenbuyorders
        self.marketBTCUSD = client.get_product_ticker("BTC-USD")
        self.marketBTCEUR = client.get_product_ticker("BTC-EUR")
        self.exchangerate = float(self.marketBTCEUR["price"]) / float(self.marketBTCUSD["price"])

    def getorders(self):
        """{'id': 'e22c9172-0276-47f7-b774-2559784c26aa', 'price': '999.85000000', 
            'size': '0.01000000', 'product_id': 'BTC-EUR', 'side': 'buy', 
            'type': 'limit', 'time_in_force': 'GTC', 'post_only': True,
            'created_at': '2018-08-17T14:57:18.551778Z', 'fill_fees': '0.0000000000000000',
            'filled_size': '0.00000000', 'executed_value': '0.0000000000000000', 
            'status': 'open', 'settled': False}"""
    
        self.openorders = client.get_orders()[0]
        self.df_openorders = pd.DataFrame(self.openorders)
        #print('DAXY DEBUG', self.df_openorders)
        
    def getbalances(self):
        """{'id': '459d001f-0391-4e97-89e7-ae474275e2c9', 'currency': 'BTC',
            'balance': '0.0530287336346057', 'available': '0.0530287336346057',
            'hold': '0.0000000000000000', 'profile_id': '5100622b-3ed2-49e4-9810-c28fb96d30b3'} """

        self.balances = client.get_accounts()
        self.df_balances = pd.DataFrame(self.balances)
        #print('DAXY DEBUG', self.df_balances)

    def getclearance(self, kwargs_dict):
        try:
            orderprice = kwargs_dict['price'] * kwargs_dict['size']
            print('DAXY CM order received total price: {}'.format(orderprice))
            self.getbalances()
            self.getorders()
        except KeyError:
            print('DAXY CM KeyError calculating orderprice')
            print('DAXY CM KeyError rejecting order request')
            return False

        if float(self.df_balances[self.df_balances['currency']=='EUR'].iloc[0,0]) <= orderprice:
            print('DAXY CM insufficient funds')
            return False
        elif len(self.df_openorders[self.df_openorders['side'].str.lower() == 'buy']) >= self.maxopenbuyorders:
            print('DAXY CM maximum limit orders reached')
            return False
        else:
            return True

class GAC(gdax.AuthenticatedClient):
    def __clearingmaster__(self):
        self.cm_ = clearingmaster()
        self.timeout = 15

    def buy(self, **kwargs):
        """client.buy(size="0.005000000",
                product_id="BTC-EUR",
                side="buy",
                stp="dc",
                type="limit")
                
            from CB PRO website
                {"id": "d0c5340b-6d6c-49d9-b567-48c4bfca13d2",
                "price": "0.10000000",
                "size": "0.01000000",
                "product_id": "BTC-USD",
                "side": "buy",
                "stp": "dc",
                "type": "limit",
                "time_in_force": "GTC",
                "post_only": false,
                "created_at": "2016-12-08T20:02:28.53864Z",
                "fill_fees": "0.0000000000000000",
                "filled_size": "0.00000000",
                "executed_value": "0.0000000000000000",
                "status": "pending",
                "settled": false}"""

        kwargs["side"] = "buy"
        kwargs["type"] = "limit"
        trade_request = self.cm_.getclearance(kwargs_dict=kwargs)
        
        if trade_request == True:
            r = requests.post(self.url + '/orders',
                            data=json.dumps(kwargs),
                            auth=self.auth,
                            timeout=30)

            print(r)
            r = r.json()
            r.update({'MONGOKEY':'BUY_ORDER','timestamp':time.time()})
            db.insert_one(r)
            print('DAXY BOUGHT inserted in mongo')
            return r
        else:
            print('DAXY CM no permission')

    def sell(self, **kwargs):
        """client.sell(size="0.005000000",
                product_id="BTC-EUR",
                side="sell",
                stp="dc",
                type="limit")"""

        kwargs["side"] = "sell"
        r = requests.post(self.url + '/orders',
                          data=json.dumps(kwargs),
                          auth=self.auth,
                          timeout=30)

        print('SOLD !!')
        print(r)
        r.update({'MONGOKEY':'SELL_ORDER','timestamp':time.time()})
        db.insert_one(r.json())
        print('DAXY SOLD inserted in mongo')
        return r.json()

if __name__ == "__main__":
    client = GAC(key=key,b64secret=b64secret,
                 passphrase=passphrase,
                 api_url="https://api.pro.coinbase.com")
    
    client.__clearingmaster__() # set clearing master
    client.cm_.getorders()
    p_change = {}
    counter=0

    '''with db.watch() as stream:
        for change in stream:
            if counter >= 3 and change.get('fullDocument').get('MONGOKEY', None) == "FBP_UPDATE" and \
                    change.get('fullDocument').get('product_id', None) == "BTC-USD":

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
                print('{0} +DAXY price at : {1} - {2}'.\
                    format(time.ctime(),change.get('fullDocument').get('y',None), counter))

                p_change = change'''