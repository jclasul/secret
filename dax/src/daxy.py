from pymongo import MongoClient
from bson.objectid import ObjectId
import time
import api_keys
import requests
import gdax
import random as random
import json
import pandas as pd
import numpy as np

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
    def __init__(self, **kwargs):
        self.heartbeat_rate = 100  # seconds before we auto cancel limit order
        self.marketBTCUSD = client.get_product_ticker("BTC-USD")
        self.marketBTCEUR = client.get_product_ticker("BTC-EUR")
        self.exchangerate = float(self.marketBTCEUR["price"]) / float(self.marketBTCUSD["price"])
        print('USD EUR at : ', self.exchangerate)

    def getorders(self):
        """{'id': 'e22c9172-0276-47f7-b774-2559784c26aa', 'price': '999.85000000', 
            'size': '0.01000000', 'product_id': 'BTC-EUR', 'side': 'buy', 
            'type': 'limit', 'time_in_force': 'GTC', 'post_only': True,
            'created_at': '2018-08-17T14:57:18.551778Z', 'fill_fees': '0.0000000000000000',
            'filled_size': '0.00000000', 'executed_value': '0.0000000000000000', 
            'status': 'open', 'settled': False}"""

        self.last_10minutes = pd.DataFrame(list(db.find({'MONGOKEY':'BUY_ORDER',
                                            'product_id':'BTC-EUR','side':'buy',
                                            'timestamp' : {'$lt':self.NOW-self.heartbeat_rate,
                                                           '$gt':self.NOW-600}})\
                                        .sort([('timestamp', 1)])))    # last 10 minutes 
        print(self.last_10minutes)
        try:
            self.last_10minutes_size = self.last_10minutes['size'].astype('float').sum()
            print('DAXY L10 : {}'.format(self.last_10minutes_size))
        except KeyError:
            print('DAXY L10 no data')
            self.last_10minutes_size = 0

        openorders = client.get_orders()[0]
        try:
            df_openorders = pd.DataFrame(openorders).query('product_id == "BTC-EUR"')
            df_openorders['created_at'] = pd.to_datetime(df_openorders['created_at'])
            self.df_openorders = df_openorders
            return True     # df contains orders
        except:
            print('=/DAXY no orders on books')
            self.df_openorders = pd.DataFrame(None)
            return False    # df will be empty
        
    def getbalances(self):
        """{'id': '459d001f-0391-4e97-89e7-ae474275e2c9', 'currency': 'BTC',
            'balance': '0.0530287336346057', 'avai`lable': '0.0530287336346057',
            'hold': '0.0000000000000000', 'profile_id': '5100622b-3ed2-49e4-9810-c28fb96d30b3'} """

        balances = client.get_accounts()
        df_balances = pd.DataFrame(balances).query('currency in ("BTC","EUR")').T
        df_balances.rename(columns=df_balances.loc['currency'], inplace=True)
        
        l = ['available','balance','hold']        
        self.df_balances = df_balances.loc[l].astype('float')
        #print('DAXY DEBUG', self.df_balances)

    def getclearance(self, kwargs_dict):
        print('=/DAXY CM {}'.format(kwargs_dict['side']))
        funds_available = self.df_balances['BTC']['available'] 
        print('=/DAXY CM {} available: {}'.format(kwargs_dict['side'], funds_available))

        try:
            orderprice = kwargs_dict['price'] * kwargs_dict['size']
            print('=/DAXY CM {} received: {}'.format(kwargs_dict['side'], orderprice))
        except KeyError:
            print('=/DAXY CM {} KeyError'.format(kwargs_dict['side']))
            return False

        if kwargs_dict['side'].upper() == 'BUY':
            if self.df_balances['EUR']['available'] <= orderprice:
                return False
            else:
                self.random_size = np.maximum(funds_available * np.random.random()*0.5, 0.001)
                return True

        elif kwargs_dict['side'].upper() == 'SELL':
            if funds_available > 0:
                self.random_size = np.maximum(np.round(funds_available * np.random.random() * 0.5 ,3), 0.001)
                return True
            else:
                return False

        else:
            return False

    def heartbeat(self, **kwargs):
        self.NOW = kwargs.get('NOW', time.time())
        print('+DAXY HB {}'.format(self.NOW))        
        has_orders = self.getorders()
        if has_orders is True:
            cutoffdate = pd.to_datetime(time.time()-self.heartbeat_rate, unit='s')
            to_terminate = self.df_openorders[self.df_openorders["created_at"]<cutoffdate]['id']
            for idtoterminate in to_terminate:
                response_cancel = client.cancel_order(idtoterminate) 
                db.delete_one({'MONGOKEY':'BUY_ORDER','trade_id': idtoterminate})
                print('=DAXY CANCELLED old order : {}'.format(response_cancel))
        print('-DAXY HB')

class GAC(gdax.AuthenticatedClient):
    def __clearingmaster__(self):
        self.cm_ = clearingmaster()
        self.timeout = 15

    def ORDER(self, p_change, **kwargs):
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

        print('+DAXY ORDER : {}'.format(kwargs.get("side", None)))
        kwargs["type"] = "limit"
        kwargs["product_id"] = "BTC-EUR"
        self.cm_.getbalances()

        if kwargs["side"] == "sell":
            if kwargs["price"] <= p_change:
                print('=DAXY upper 0002 broken')
                order_price = p_change*1.006
            kwargs["price"] = np.round(kwargs['price'] * self.cm_.exchangerate, 2)

            trade_request = self.cm_.getclearance(SIDE='SC', kwargs_dict=kwargs)
            kwargs["size"] = self.cm_.random_size

        elif kwargs["side"] == "buy":
            if kwargs["price"] >= p_change:
                print('=DAXY lower 002 broken')
                order_price = p_change*0.996
            kwargs["price"] = np.round(kwargs['price'] * self.cm_.exchangerate, 2)
            
            
        trade_request = self.cm_.getclearance(SIDE, kwargs_dict=kwargs)
            
        kwargs["size"] = self.cm_.random_size
        
        if trade_request == True:
            r = requests.post(self.url + '/orders',
                            data=json.dumps(kwargs),
                            auth=self.auth,
                            timeout=30)

            rjson = r.json()
            print(rjson)
            rjson.update({'MONGOKEY':'BUY_ORDER',
                          'timestamp':time.time(),
                          'trade_id':rjson['id'],
                          'strategy':'ISS'})
            floatlist = ['price','size','executed_value','fill_fees','filled_size']
            for itemfloat in floatlist:
                rjson[itemfloat] = float(rjson.get(itemfloat, 0))
            rjson.pop('id', None)
            db.insert_one(rjson)
            print('-DAXY OP {}'.format(kwargs["side"]))
            return rjson
        else:
            print('=DAXY CM {} : NA'.format(kwargs['side']))

if __name__ == "__main__":
    client = GAC(key=key,b64secret=b64secret,
                 passphrase=passphrase,
                 api_url="https://api.pro.coinbase.com")
    
    client.__clearingmaster__() # set clearing master
    p_change = 0
    counter = 0
    order_price = 0
    hbcounter = time.time()
    random_wait = np.random.randint(4,40)
    price_target_lower = 'yhat_lower_fcst_002'
    price_target_upper = 'yhat_upper_fcst_0002'

    with db.watch() as stream:
        for change in stream:
            if change.get('fullDocument', None) is not None:
                if change.get('fullDocument').get('MONGOKEY', None) is not "BUY_ORDER":
                    if change.get('fullDocument').get('MONGOKEY') == "FBP_UPDATE" and \
                            change.get('fullDocument').get('product_id', None) == "BTC-USD":

                        keys = ['_id',
                                'yhat_lower_fcst_0002', 'yhat_lower_fcst_002',
                                'yhat_upper_fcst_002', 'yhat_upper_fcst_0002']
                        print(time.ctime(),[change.get('fullDocument').get(ckey,None) for ckey in keys]) 

                        order_price = change.get('fullDocument').get(price_target_lower, 1)
                        sell_price = change.get('fullDocument').get(price_target_upper, 1)

                        counter = 0

                    if change.get('fullDocument').get('MONGOKEY', None) == "MARKET_UPDATE" and \
                            change.get('fullDocument').get('product_id', None) == "BTC-USD":

                        counter += 1
                        p_change = float(change.get('fullDocument').get('y',0))
                        print('{0} +DAXY price at : {1} - {2}'.\
                            format(time.ctime(),p_change, counter))

            if random.random() < 0.30:
                NOW = time.time()
                if NOW - hbcounter >= random_wait:
                    client.cm_.heartbeat(NOW=NOW)
                    hbcounter = NOW
                    random_wait = np.random.randint(4,40)

                    if p_change is not 0 and order_price is not 0 \
                            and sell_price is not 0 and counter < 10:  
                        print('+DAXY ORDER LOOP')
                        random_order = np.random.random()

                        if random_order > 0.5:
                            client.ORDER(p_change,size=random_size, price=order_price,
                                        product_id="BTC-EUR",side="buy",type="limit") 
                        else:
                            client.ORDER(p_change,size=random_size,price=sell_price,
                                        product_id="BTC-EUR",side="sell",type="limit")
                                        
                        counter += 1
                                                
                    else:
                        print('DAXY prices at zero')