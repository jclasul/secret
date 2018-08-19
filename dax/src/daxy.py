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

# specify the database and collection`
db = MongoClient(api.mongo).gdax.gdaxws
print("connected to mongo collection")

# and store in local variables
key = api.api_key
b64secret = api.secret_key
passphrase = api.passphrase

class clearingmaster():
    def __init__(self, client):
        self.client = client
        self.heartbeat_rate = 100  # seconds before we auto cancel limit order
        self.marketBTCUSD = self.client.get_product_ticker("BTC-USD")
        self.marketBTCEUR = self.client.get_product_ticker("BTC-EUR")
        self.exchangerate = float(self.marketBTCEUR["price"]) / float(self.marketBTCUSD["price"])
        
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

        try:
            self.last_10minutes_size = self.last_10minutes['size'].astype('float').sum()
            print('DAXY L10 : {}'.format(self.last_10minutes_size))
        except KeyError:
            print('DAXY L10 no data')
            self.last_10minutes_size = 0

        openorders = self.client.get_orders()[0]
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

        balances = self.client.get_accounts()
        df_balances = pd.DataFrame(balances).query('currency in ("BTC","EUR")').T
        df_balances.rename(columns=df_balances.loc['currency'], inplace=True)
        
        l = ['available','balance','hold']        
        self.df_balances = df_balances.loc[l].astype('float')
        #print('DAXY DEBUG', self.df_balances)

    def getclearance(self, kwargs_dict, price_trend_0002, price_trend_002):
        self.getbalances()
        print('=/DAXY CM {}'.format(kwargs_dict['side']))
        funds_available_btc = self.df_balances['BTC']['available'] 
        funds_available_eur = self.df_balances['EUR']['available'] 
        print('=/DAXY CM {} funds BTC available: {:0.3f}'.format(kwargs_dict['side'], funds_available_btc))
        self.order_size = np.maximum(funds_available_btc * np.random.random() * 0.3, 0.001)

        try:
            orderprice = kwargs_dict['price'] * self.order_size
            print('=/DAXY CM {} received: {:0.2f}'.format(kwargs_dict['side'], orderprice))
        except KeyError:
            print('=/DAXY CM {} KeyError'.format(kwargs_dict['side']))
            return False

        if kwargs_dict['side'] == 'buy' and self.df_balances['EUR']['available'] > orderprice + 0.5 \
                and kwargs_dict["price"] <= price_trend_0002:
            self.order_size = np.maximum(funds_available_eur / kwargs_dict["price"], 0.001)
            return True

        if kwargs_dict['side'] == 'sell' and funds_available_btc > 0.001 \
                and kwargs_dict["price"] >= price_trend_0002:
            return True
        
        return False

    def heartbeat(self, **kwargs):
        self.NOW = kwargs.get('NOW', time.time())
        print('+DAXY HB {}'.format(self.NOW))        
        has_orders = self.getorders()
        if has_orders is True:
            cutoffdate = pd.to_datetime(self.NOW-self.heartbeat_rate, unit='s')
            to_terminate = self.df_openorders[self.df_openorders["created_at"]<cutoffdate]['id']
            for idtoterminate in to_terminate:
                response_cancel = self.client.cancel_order(idtoterminate) 
                db.delete_one({'MONGOKEY':'BUY_ORDER','trade_id': idtoterminate})
                print('=DAXY CANCELLED old order : {}'.format(response_cancel))
        print('-DAXY HB')

class orderpicker():
    def __init__(self, client):
        self.cm_ = clearingmaster(client)

    def storefbp(self, fbp_change):
        self.yhat_lower_fcst_0002 = fbp_change.get('yhat_lower_fcst_0002', 0) 
        self.yhat_lower_fcst_002 = fbp_change.get('yhat_lower_fcst_002', 0) 
        self.yhat_upper_fcst_0002 = fbp_change.get('yhat_upper_fcst_0002', 0) 
        self.yhat_upper_fcst_002 = fbp_change.get('yhat_upper_fcst_002', 0) 
        self.trend_fcst_0002 = fbp_change.get('trend_fcst_0002', 0) 
        self.trend_fcst_002 = fbp_change.get('trend_fcst_002', 0) 

        print('DAXY FBP update : ',
              self.yhat_lower_fcst_0002,self.yhat_lower_fcst_002,self.yhat_upper_fcst_0002,
              self.yhat_upper_fcst_002,self.trend_fcst_0002,self.trend_fcst_002)

        if self.yhat_lower_fcst_002 > 0 and self.yhat_upper_fcst_0002 > 0:
            return True
        else:
            return False    

    def makeorder(self, lastknowprice, **kwargs):
        print('+DAXY ORDER : {}'.format(kwargs.get("side", None)))

        if kwargs["side"] == "sell" and self.yhat_upper_fcst_002 <= lastknowprice:
            print('=DAXY upper 002 broken')
            kwargs["price"] = lastknowprice*1.0035
        elif kwargs["side"] == "sell":
            kwargs["price"] = self.yhat_upper_fcst_002

        if kwargs["side"] == "buy" and self.yhat_lower_fcst_002 >= lastknowprice:
            print('=DAXY lower 002 broken')
            kwargs["price"] = lastknowprice*0.997     
        elif kwargs["side"] == "buy":
            kwargs["price"] = self.yhat_lower_fcst_002

        kwargs["price"] = np.round(kwargs["price"] * self.cm_.exchangerate, 2)

        trade_request = self.cm_.getclearance(kwargs_dict=kwargs, 
                                              price_trend_0002=self.trend_fcst_0002, 
                                              price_trend_002=self.trend_fcst_002)        
    
        if trade_request is True:
            kwargs["size"] = np.round(self.cm_.order_size, 3)
            return True, kwargs       
        else:
            return False, kwargs  

class GAC(gdax.AuthenticatedClient): 
    def ORDER(self, **kwargs):
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

        kwargs["type"] = "limit"
        kwargs["post_only"] = True
        kwargs["product_id"] = "BTC-EUR"        
        print("=DAXY TRADING")

        kwargs.pop('trend_price', None)
        r = requests.post(self.url + '/orders',
                        data=json.dumps(kwargs),
                        auth=self.auth,
                        timeout=30)

        rjson = r.json()
        print(rjson)
        try:
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
        except KeyError:
            print("-DAXY OP KeyError..")
            db.insert_one({'MONGOKEY':'ERROR_LOG',
                           'ERROR_MSG':rjson,
                           'timestamp':time.time()})

class mongowatcher():
    def __init__(self):
        self.client = client = GAC(key=key,b64secret=b64secret,
                                    passphrase=passphrase,
                                    api_url="https://api.pro.coinbase.com")
    
        self.op_ = orderpicker(self.client)        
        self.lastknowprice = 0
        self.counter = 0    
        self.hbcounter = time.time()

    def watcher(self, random_wait=10, fbp_update=False):
        NOW = time.time()
        with db.watch() as stream:
            for change in stream:
                if change.get('fullDocument', None) is not None:
                    if change.get('fullDocument').get('MONGOKEY', None) is not "BUY_ORDER":
                        if change.get('fullDocument').get('MONGOKEY') == "FBP_UPDATE" and \
                                change.get('fullDocument').get('product_id', None) == "BTC-USD":

                            fbp_update = self.op_.storefbp(fbp_change=change.get('fullDocument', {}))
                            self.counter = 0

                        if change.get('fullDocument').get('MONGOKEY', None) == "MARKET_UPDATE" and \
                                change.get('fullDocument').get('product_id', None) == "BTC-USD":

                            self.counter += 1
                            self.lastknowprice = float(change.get('fullDocument').get('y',0))
                            print('{0} +DAXY price at : {1} - {2}'.\
                                format(NOW, self.lastknowprice, self.counter))

                        if random.random() < 0.1:
                            NOW = time.time()
                            if NOW - self.hbcounter >= random_wait:
                                self.op_.cm_.heartbeat(NOW=NOW)
                                self.hbcounter = NOW
                                random_wait = np.random.randint(4,40)

                            if self.lastknowprice is not 0 and fbp_update is True and self.counter < 30:
                                print('+DAXY ORDER LOOP')
                                random_order = np.random.random()

                                if random_order > 0.5:
                                    random_side = 'buy'
                                else:
                                    random_side = 'sell'
                                                
                                flag, TK = self.op_.makeorder(lastknowprice=self.lastknowprice,side=random_side)

                                if flag is True:
                                    self.client.ORDER(price=TK['price'], side=TK['side'], size=TK['size']) 
                                else:
                                    print("DAXY NA")

                                self.counter += 1
                                                        
                            else:
                                print(NOW, 'DAXY prices at zero')

if __name__ == "__main__":
    print('DAXY starting')
    mw_ = mongowatcher()
    print(mw_.op_.cm_.exchangerate)
    mw_.watcher()