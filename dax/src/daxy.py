from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
import time
import api_keys
import requests
import gdax
import random as random
import json
import pandas as pd
import numpy as np
import sys

from threading import Thread

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
        self.longthreshold = 0.35
        self.getexchangerate()

    def getexchangerate(self):
        self.marketBTCUSD = float(self.client.get_product_ticker("BTC-USD")["price"])
        self.marketBTCEUR = float(self.client.get_product_ticker("BTC-EUR")["price"])
        self.exchangerate = self.marketBTCEUR / self.marketBTCUSD
        print('DAXY USD-EUR: {:0.3f}'.format(self.exchangerate))
        
    def getorders(self):
        """{'id': 'e22c9172-0276-47f7-b774-2559784c26aa', 'price': '999.85000000', 
            'size': '0.01000000', 'product_id': 'BTC-EUR', 'side': 'buy', 
            'type': 'limit', 'time_in_force': 'GTC', 'post_only': True,
            'created_at': '2018-08-17T14:57:18.551778Z', 'fill_fees': '0.0000000000000000',
            'filled_size': '0.00000000', 'executed_value': '0.0000000000000000', 
            'status': 'open', 'settled': False}"""

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
        balance_long = self.df_balances.loc['balance']['EUR']
        balance_short = self.df_balances.loc['balance']['BTC'] * self.requestedprice
        self.balance_longshort = balance_long + balance_short
        self.ratio_long = float(balance_long / self.balance_longshort)
        self.ratio_longshort = float(balance_long) / float(balance_short)
        self.ratio_long_oke = self.ratio_long > self.longthreshold
        print('=/DAXY CM GB {:0.2f} = {}'.format(self.ratio_long, self.ratio_long_oke))
        #print('DAXY DEBUG', self.df_balances)

    def getclearance_balances(self, kwargs_dict):
        self.getbalances()                
        funds_available_btc = self.df_balances['BTC']['available'] 
        funds_available_eur = self.df_balances['EUR']['available'] 
        print('=/DAXY CM GC {} BTC: {:0.3f} EUR: {:0.0f}'\
            .format(kwargs_dict['side'], funds_available_btc, funds_available_eur))

        r_size = random.randint(100,2000)/10000 #between 1 and 20%
        if kwargs_dict["side"] == "buy":
            self.order_size = np.maximum(
                    funds_available_eur / self.requestedprice * r_size, 0.001)

        elif kwargs_dict["side"] == "sell":
            self.order_size = np.maximum(funds_available_btc * r_size, 0.001)

        try:
            orderprice = self.requestedprice * self.order_size
            print('=/DAXY CM GC {} received: {:0.0f} EUR {:0.0f} USD'\
                .format(kwargs_dict['side'], self.requestedprice, self.requestedprice / self.exchangerate))
        except KeyError:
            print('=/DAXY CM GC {} KeyError'.format(kwargs_dict["side"]))
            return False, None, None, None

        if self.requestedprice < self.marketBTCEUR * 0.8:
            return False, None, None, None
        
        return True, orderprice, funds_available_eur, funds_available_btc

    def getclearance(self, kwargs_dict, price_trend_0002, price_trend_002):
        print('=/DAXY CM GC {}'.format(kwargs_dict['side']))
        self.requestedprice = kwargs_dict.get("price", 0)   #price already in EUR
        price_trend_0002 = price_trend_0002 * self.exchangerate #convert fbp to EUR
        price_trend_002 = price_trend_002 * self.exchangerate       

        if kwargs_dict['side'] == 'buy':
            if self.requestedprice < price_trend_0002:
                return_, orderprice, funds_available_eur, funds_available_btc \
                        = self.getclearance_balances(kwargs_dict)

                if self.df_balances['EUR']['available'] > orderprice + 0.5 and return_ is True:                
                    if self.ratio_long_oke is True:                        
                        return True
                        
                    else:
                        NA = 'ratio long not oke {:0.2f}'.format(self.ratio_long)
                else:
                    NA = 'unsufficient funds: {:0.0f} EUR'.format(funds_available_eur)
            else:
                NA = 'below trend {:0.0f} EUR'.format(price_trend_0002)

        if kwargs_dict['side'] == 'sell':
            if self.requestedprice > price_trend_0002:
                return_, orderprice, funds_available_eur, funds_available_btc \
                        = self.getclearance_balances(kwargs_dict)

                if funds_available_btc > 0.001:                
                    return True
                else:
                    NA = 'unsufficient funds: {:0.0f} BTC'.format(funds_available_btc)
            else:
                NA = 'below trend {:0.0f} EUR'.format(price_trend_0002)
        
        print('=/DAXY CM GC {} NA = {}'.format(kwargs_dict["side"], NA))
        return False

    def heartbeat(self, **kwargs):
        self.NOW = kwargs.get('NOW', time.time())
        print('+DAXY HB')        
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
        self.lastknowprice = 0
        self.yhat_lower_fcst_0002 = 0
        self.yhat_lower_fcst_002 = 0
        self.yhat_upper_fcst_0002 = 0
        self.yhat_upper_fcst_002 = 0
        self.trend_fcst_0002 = 0
        self.trend_fcst_002 = 0

    def storefbp(self, fbp_change):
        self.yhat_lower_fcst_0002 = fbp_change.get('yhat_lower_fcst_0002', 0) 
        self.yhat_lower_fcst_002 = fbp_change.get('yhat_lower_fcst_002', 0) 
        self.yhat_upper_fcst_0002 = fbp_change.get('yhat_upper_fcst_0002', 0) 
        self.yhat_upper_fcst_002 = fbp_change.get('yhat_upper_fcst_002', 0) 
        self.trend_fcst_0002 = fbp_change.get('trend_fcst_0002', 0) 
        self.trend_fcst_002 = fbp_change.get('trend_fcst_002', 0) 

        print('DAXY FBP update [USD] L3:{:0.0f}|L2:[{:0.0f}]\tT3:{:0.0f}|T2:{:0.0f}\tU2:[{:0.0f}]|U3:{:0.0f}'\
                .format(self.yhat_lower_fcst_0002,self.yhat_lower_fcst_002,self.trend_fcst_0002,
                        self.trend_fcst_002,self.yhat_upper_fcst_002,self.yhat_upper_fcst_0002))

        if self.yhat_lower_fcst_002 > 0 and self.yhat_upper_fcst_0002 > 0:
            return True
        else:
            return False    

    def storemarket(self, market_change):
        self.lastknowprice = market_change.get('y', 0)
        print('{}+DAXY price at : {:0.0f} USD'.format(market_change.get('time', None),self.lastknowprice))

        if self.lastknowprice > self.cm_.marketBTCUSD * 0.8:
            return True
        else:
            return False

    def makeorder(self, **kwargs):
        print('+DAXY ORDER {}'.format(kwargs.get("side", None)))
        r = random.randint(99980,100020)/100000
        if kwargs["side"] == "sell" and self.yhat_upper_fcst_002 <= self.lastknowprice:
            print('=DAXY upper 002 broken')
            self.cm_.heartbeat()
            kwargs["price"] = self.lastknowprice*1.0035
        elif kwargs["side"] == "sell":
            kwargs["price"] = self.yhat_upper_fcst_002 * r
        
        if kwargs["side"] == "buy" and self.yhat_lower_fcst_002 >= self.lastknowprice:
            print('=DAXY lower 002 broken')
            self.cm_.heartbeat()
            kwargs["price"] = self.lastknowprice * 0.99626401  
        elif kwargs["side"] == "buy":
            kwargs["price"] = self.yhat_lower_fcst_002 * r

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
        kwargs["time_in_force"] = "GTT"
        kwargs["cancel_after"] = "min" 
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
        self.hbcounter = time.time()
        self.ordertimer = self.hbcounter
        self._update = False
        self.order_interval = 30
        self.UPDATE_HANDLER = {'FBP_UPDATE':self.op_.storefbp,
                                'MARKET_UPDATE':self.op_.storemarket,
                                'None':self.error_pass}

    def error_pass(self):
        print('DAXY ERROR_PASS')

    def caller(self):
        NOW = time.time()
        if NOW - self.hbcounter >= 100:
            self.op_.cm_.heartbeat(NOW=NOW)
            self.hbcounter = NOW

        if self._update is True and NOW - self.ordertimer > self.order_interval:
            if random.random() > 0.60:
                print('+DAXY ORDER LOOP')
                random_order = np.random.random()

                if random_order > 0.5:
                    random_side = 'buy'
                else:
                    random_side = 'sell'
                                
                flag, TK = self.op_.makeorder(side=random_side)

                if flag is True:
                    self.client.ORDER(price=TK['price'], side=TK['side'], size=TK['size']) 
                else:
                    pass

                self.ordertimer = NOW
            else:
                self.ordertimer = NOW

            self.order_interval = 8

        else:
            pass

    def watcher(self):
        try:
            with db.watch(
                    [{'$match':{'operationType': "insert"}},{'$replaceRoot':{'newRoot':'$fullDocument'}},
                     {'$match':{'product_id':'BTC-USD'}},
                     {'$match':{'MONGOKEY':{'$in': ['FBP_UPDATE', 'MARKET_UPDATE']}}}]) as stream:
                for insert_change in stream:
                    change_type = insert_change.get('MONGOKEY', 'None') # either FBP_UPDATE or MARKET_UPDATE
                    self._update = self.UPDATE_HANDLER[change_type](insert_change)                    
        except pymongo.errors.PyMongoError:
            # The ChangeStream encountered an unrecoverable error or the
            # resume attempt failed to recreate the cursor.
            print('DAXY MONGO WATCH ERROR')
            pass

    def caller_wrapper(self):
        exchange_update_counter = 0
        while True:
            self.caller()
            time.sleep(1)

            exchange_update_counter += 1
            if exchange_update_counter > 600:
                self.op_.cm_.getexchangerate()
                exchange_update_counter = 0
                try:
                    db.insert_one({'MONGOKEY':'BALANCE',
                                   'timestamp':time.time(),
                                   'product_id':'BTC-USD',
                                   'balance':self.op_.cm_.balance_longshort})
                except Exception:
                    pass

if __name__ == "__main__":
    mw_ = mongowatcher()
    print(mw_.op_.cm_.exchangerate)    

    T1 = Thread(target = mw_.watcher)
    T2 = Thread(target = mw_.caller_wrapper)

    T2.start()
    T1.start()