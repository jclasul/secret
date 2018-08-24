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
        self.client             =   client
        self.heartbeat_rate     =   100 # seconds before we auto cancel limit order
        self.longshort_adj      =   0.5 # 50-50 odds of buy-sell
        self.longthreshold      =   0.35
        self.booster            =   1.00
        self.minimum_order      =   {'BTC-EUR':0.001,'BTC-USD':0.001,
                                     'ETH-EUR':0.01,'ETH-USD':0.01}

        self.getexchangerate()
        self.getbalances()

    def getexchangerate(self):  # __F:caller
        histBTCEUR              =   float(self.client.get_product_ticker("BTC-EUR")["price"])
        histETHUSD              =   float(self.client.get_product_ticker("ETH-USD")["price"])
        histBTCUSD              =   float(self.client.get_product_ticker("BTC-USD")["price"])
        histETHEUR              =   float(self.client.get_product_ticker("ETH-EUR")["price"])

        self.historicrates      =   {'BTC-EUR':histBTCEUR,
                                     'ETH-USD':histETHUSD,
                                     'BTC-USD':histBTCUSD,
                                     'ETH-EUR':histETHEUR}

        self.exchangerate       =   histBTCEUR / histBTCUSD
        print('DAXY USD-EUR: {:0.3f}'.format(self.exchangerate))
        
    def getorders(self):                                                # __F:HB
        """{'id': 'e22c9172-0276-47f7-b774-2559784c26aa', 'price': '999.85000000', 
            'size': '0.01000000', 'product_id': 'BTC-EUR', 'side': 'buy', 
            'type': 'limit', 'time_in_force': 'GTC', 'post_only': True,
            'created_at': '2018-08-17T14:57:18.551778Z', 'fill_fees': '0.0000000000000000',
            'filled_size': '0.00000000', 'executed_value': '0.0000000000000000', 
            'status': 'open', 'settled': False}"""

        openorders              =   self.client.get_orders()[0]
        try:
            df_openorders = pd.DataFrame(openorders).query('product_id == "BTC-EUR" or product_id == "ETH-EUR"')
            df_openorders['created_at'] = pd.to_datetime(df_openorders['created_at'])
            self.df_openorders  =   df_openorders
            return True     # df contains orders
        except:
            print('=/DAXY no orders on books')
            self.df_openorders  =   pd.DataFrame(None)
            return False    # df will be empty
        
    def getbalances(self):  # called before every order                 # __F:getclearance_balance
        """{'id': '459d001f-0391-4e97-89e7-ae474275e2c9', 'currency': 'BTC',
            'balance': '0.0530287336346057', 'avai`lable': '0.0530287336346057',
            'hold': '0.0000000000000000', 'profile_id': '5100622b-3ed2-49e4-9810-c28fb96d30b3'} """

        balances                =   self.client.get_accounts()
        df_balances             =   pd.DataFrame(balances)\
                                        .query('currency in ("BTC","EUR","ETH")').T
        df_balances             =   df_balances\
                                        .rename(columns=df_balances.loc['currency'])
        
        l = ['available','balance','hold']        
        self.df_balances        =   df_balances.loc[l].astype('float')

        balance_long            =   self.df_balances.loc['balance']['EUR']
        balance_short           =   self.df_balances.loc['balance']['BTC'] * self.historicrates['BTC-EUR'] \
                                        + self.df_balances.loc['balance']['ETH'] * self.historicrates['ETH-EUR']

        self.balance_longshort  =   balance_long + balance_short
        self.ratio_long         =   float(balance_long / self.balance_longshort)
        self.ratio_short        =   float(balance_short / self.balance_longshort)  
        self.ratio_long_oke     =   self.ratio_long > self.longthreshold

        if 0.3 < self.ratio_short < 0.65:
            self.longshort_adj = 0.5
        elif self.ratio_short < 0.3:
            self.longshort_adj  =   np.maximum(self.ratio_short, 0.05)
        elif self.ratio_short > 0.65:
            self.longshort_adj  =   0.7
        
        print('=/DAXY CM GB l:{:0.2f} s:{:0.2f} = {}'.format(self.ratio_long, self.ratio_short, self.ratio_long_oke))
        #print('DAXY DEBUG', self.df_balances)

    def getclearance_balances(self, kwargs_dict):     # ALL IN EUR      # __F:getclearance @__:getbalance
        self.getbalances()                  # __func_order_BALANCE

        order_side              =   kwargs_dict["side"]
        product_id              =   kwargs_dict["product_id"]   
        crypto_id               =   product_id.split('-')[0]              
        funds_available_crypto  =   self.df_balances[crypto_id]['available'] 
        funds_available_eur     =   self.df_balances['EUR']['available'] 
        minimum_order           =   self.minimum_order[product_id]
        r_size                  =   random.randint(100,2000)/10000 #between 1 and 20%

        print('=/DAXY CM GC {} CRYPTO: {:0.3f} EUR: {:0.0f}'\
            .format(order_side, funds_available_crypto, funds_available_eur))
        
        if order_side == "buy":
            self.order_size     = np.maximum(
                    funds_available_eur / self.requestedprice * r_size, minimum_order)

        elif order_side == "sell":
            self.order_size     = np.maximum(funds_available_crypto * r_size, minimum_order)

        try:
            orderprice          = self.requestedprice * self.order_size
            print('=/DAXY CM GC {} received: {:0.0f} EUR {:0.0f} USD'\
                .format(kwargs_dict['side'], self.requestedprice, self.requestedprice / self.exchangerate))
        except KeyError:
            print('=/DAXY CM GC {} KeyError'.format(kwargs_dict["side"]))
            return False, 0, 0, 0

        if self.requestedprice < self.historicrates[product_id] * 0.8:
            return False, 0, 0, 0
        
        return True, orderprice, funds_available_eur, funds_available_crypto

    def getclearance(self, kwargs_dict, price_trend_0002, price_trend_002): # __F:makeorder @__:getclearance_balance
        order_side              =   kwargs_dict.get('side', None)     
        product_id              =   kwargs_dict["product_id"]   
        self.requestedprice     =   kwargs_dict.get("price", 0) #price already in EUR

        print('=/DAXY CM GC {}'.format(kwargs_dict['side']))
                
        if order_side == 'buy':
            if self.requestedprice < price_trend_0002:
                return_, orderprice, funds_available_eur, funds_available_crypto \
                                = self.getclearance_balances(kwargs_dict)   # __order_func_balance

                if self.df_balances['EUR']['available'] > orderprice + 0.5 and return_ is True:                
                    if self.ratio_long_oke is True:                        
                        return True
                        
                    else:
                        NA = 'ratio long not oke {}'.format('[placeholder]')
                else:
                    NA = 'unsufficient funds: {:0.0f} EUR'.format(funds_available_eur)
            else:
                NA = 'below trend {:0.0f} EUR'.format(price_trend_0002)

        if order_side == 'sell':
            if self.requestedprice > price_trend_0002:
                return_, orderprice, funds_available_eur, funds_available_crypto \
                                = self.getclearance_balances(kwargs_dict)   # __order_func_balance

                if funds_available_crypto > self.minimum_order[product_id]:                
                    return True
                else:
                    NA = 'unsufficient funds: {:0.0f} CRYPTO'.format(funds_available_crypto)
            else:
                NA = 'above trend {:0.0f} EUR'.format(price_trend_0002)
        
        print('=/DAXY CM GC {} NA = {}'.format(order_side, NA))
        return False

    def heartbeat(self, **kwargs):  # __F:caller @__:getorders
        self.NOW                = kwargs.get('NOW', time.time())
        heartbeat_rate          = kwargs.get('heartbeat_rate', self.heartbeat_rate)
        product_id              = kwargs.get('product_id', 'BTC-EUR')
        order_side              = kwargs.get('side', 'buy')

        print('+DAXY HB')        

        has_orders              = self.getorders()
        if has_orders is True:
            cutoffdate          = pd.to_datetime(self.NOW-heartbeat_rate, unit='s')
            to_terminate        = self.df_openorders[(self.df_openorders["created_at"]<cutoffdate) & \
                                                    (self.df_openorders["product_id"]==product_id) & \
                                                    (self.df_openorders["side"]==order_side)]['id']
            for idtoterminate in to_terminate:
                response_cancel = self.client.cancel_order(idtoterminate) 
                db.delete_one({'MONGOKEY':'BUY_ORDER','trade_id': idtoterminate})
                print('=DAXY CANCELLED old order : {}'.format(response_cancel))
        print('-DAXY HB')

class orderpicker():
    def __init__(self, client):
        self.cm_                    =   clearingmaster(client)

        self.lastknowprice          =   {'BTC-USD':0,'ETH-USD':0}
        self.yhat_lower_fcst_0002   =   {'BTC-USD':0,'ETH-USD':0}
        self.yhat_lower_fcst_002    =   {'BTC-USD':0,'ETH-USD':0}
        self.yhat_upper_fcst_0002   =   {'BTC-USD':0,'ETH-USD':0}
        self.yhat_upper_fcst_002    =   {'BTC-USD':0,'ETH-USD':0}
        self.trend_fcst_0002        =   {'BTC-USD':0,'ETH-USD':0}
        self.trend_fcst_002         =   {'BTC-USD':0,'ETH-USD':0}
        
        self.trend_002_counter      =   {'BTC-USD+sell':0,'BTC-USD+buy':0,
                                         'ETH-USD+sell':0,'ETH-USD+buy':0}

    def storefbp(self, fbp_change): # __F:watcher 
        product_id                  =   fbp_change.get('product_id', 'None')

        yhat_lower_fcst_0002        =   fbp_change.get('yhat_lower_fcst_0002', 0) 
        yhat_lower_fcst_002         =   fbp_change.get('yhat_lower_fcst_002', 0) 
        yhat_upper_fcst_0002        =   fbp_change.get('yhat_upper_fcst_0002', 0) 
        yhat_upper_fcst_002         =   fbp_change.get('yhat_upper_fcst_002', 0) 
        trend_fcst_0002             =   fbp_change.get('trend_fcst_0002', 0) 
        trend_fcst_002              =   fbp_change.get('trend_fcst_002', 0) 

        self.yhat_lower_fcst_0002.update({product_id:yhat_lower_fcst_0002})
        self.yhat_lower_fcst_002.update({product_id:yhat_lower_fcst_002})
        self.yhat_upper_fcst_0002.update({product_id:yhat_upper_fcst_0002})
        self.yhat_upper_fcst_002.update({product_id:yhat_upper_fcst_002})
        self.trend_fcst_0002.update({product_id:trend_fcst_0002})
        self.trend_fcst_002.update({product_id:trend_fcst_002})

        print('DAXY {} FBP update [USD] L3:{:0.0f}|L2:[{:0.0f}]\tT3:{:0.0f}|T2:{:0.0f}\tU2:[{:0.0f}]|U3:{:0.0f}'\
                .format(product_id,
                        yhat_lower_fcst_0002,   yhat_lower_fcst_002,
                        trend_fcst_0002,        trend_fcst_002,
                        yhat_upper_fcst_002,    yhat_upper_fcst_0002))

        if yhat_lower_fcst_002 > 0 and yhat_upper_fcst_0002 > 0:
            return True
        else:
            return False    

    def storemarket(self, market_change):   # __F:watcher
        product_id                  =   market_change.get('product_id', None)
        lastknowprice               =   float(market_change.get('y', 0))

        self.lastknowprice.update({product_id:lastknowprice})
        print('{}+DAXY {} price at : {:0.0f} USD'\
            .format(market_change.get('time', None), product_id, lastknowprice))

        if lastknowprice > self.cm_.exchangerate * 0.8:
            return True
        else:
            return False

    def makeorder(self, **kwargs):      # takes in USD converts to EUR  # __F:caller @__:getclearance 
        order_side                  =   kwargs.get("side", None)
        product_id                  =   kwargs.get('product_id', None)  # !! BTC-USD and ETH-USD !!
        product_id_adj              =   '+'.join([product_id,order_side])
        product_id_EUR              =   product_id.split('-')[0] + '-EUR'        
        r                           =   random.randint(99960,100040)/100000
        lastknowprice               =   self.lastknowprice[product_id]
        yhat_lower_fcst_002         =   self.yhat_lower_fcst_002[product_id]
        yhat_upper_fcst_002         =   self.yhat_upper_fcst_002[product_id]
        trend_fcst_0002             =   self.trend_fcst_0002[product_id]    *   self.cm_.exchangerate
        trend_fcst_002              =   self.trend_fcst_002[product_id]     *   self.cm_.exchangerate    

        print('+DAXY {} ORDER {} - Tcnt:{}'.format(product_id, order_side, self.trend_002_counter[product_id_adj]))

        if 120 < self.trend_002_counter[product_id_adj] < 200:
            print('==DAXY long trend exposure adjustment')
            if order_side == "sell":
                kwargs["price"] = lastknowprice * 1.0005
            else:
                kwargs["price"] = lastknowprice * 0.9995
        elif self.trend_002_counter[product_id_adj] > 200:
            print('==|DAXY end of long term trend exposure adjustment')
            self.trend_002_counter[product_id_adj] = 0
        
        if order_side == "sell" and self.trend_002_counter[product_id_adj] < 120:
            if yhat_upper_fcst_002 <= lastknowprice:   # make price __SELL
                print('=DAXY upper 002 broken')
                self.trend_002_counter[product_id_adj] += 1                
                self.cm_.heartbeat(heartbeat_rate=5, product_id=product_id_EUR, order_side=order_side)  
                kwargs["price"]         =   lastknowprice * 1.00225
            elif self.cm_.ratio_short > 0.55:
                print('=DAXY ratio short too high, reducing exposure')
                kwargs["price"]         =   ((trend_fcst_002 - yhat_upper_fcst_002)/5 * random.random()) + yhat_upper_fcst_002
            else:
                kwargs["price"]         =   yhat_upper_fcst_002 * r
                self.trend_002_counter[product_id_adj] = 0
        
        if order_side == "buy":
            if yhat_lower_fcst_002 >= lastknowprice:    # make price __BUY
                print('=DAXY lower 002 broken')
                self.trend_002_counter[product_id_adj] += 1
                kwargs["price"]         =   lastknowprice * 0.99626401  
            elif self.cm_.ratio_short < 0.3:
                print("=DAXY ratio long low, increasing exposure")
                kwargs["price"]         =   ((trend_fcst_002 - yhat_lower_fcst_002)/3 * random.random()) + yhat_lower_fcst_002
            else:
                kwargs["price"]         =   yhat_lower_fcst_002 * r
                self.trend_002_counter[product_id_adj] = 0

        # !! price converted to EUR !!
        kwargs["price"]             =   np.round(kwargs["price"] * self.cm_.exchangerate, 2)
        kwargs["product_id"]        =   product_id_EUR  # also convert product id to EUR

        trade_request = self.cm_.getclearance(kwargs_dict=kwargs, 
                                              price_trend_0002=trend_fcst_0002, 
                                              price_trend_002=trend_fcst_002)
                                              # __order_func_clearance (=EUR)

        if trade_request is True:
            kwargs["size"]          =   np.round(self.cm_.order_size, 3)
            return True, kwargs       
        else:
            return False, kwargs    # return_ORDER_placer__

class GAC(gdax.AuthenticatedClient): 
    def ORDER(self, **kwargs):  # __F:caller @__:TRADE_GDAX_ACTUALY
        kwargs["type"]              =   "limit"
        kwargs["post_only"]         =   True  
        kwargs["time_in_force"]     =   "GTT"
        kwargs["cancel_after"]      =   "min" 
        print("=DAXY TRADING")

        r = requests.post(self.url + '/orders',
                        data=json.dumps(kwargs),
                        auth=self.auth,
                        timeout=30)

        rjson = r.json()
        print(rjson)
        try:
            rjson.update({'MONGOKEY'    :   'BUY_ORDER',
                          'timestamp'   :   time.time(),
                          'trade_id'    :   rjson['id'],
                          'strategy'    :   'ISS'})

            floatlist = ['price','size','executed_value','fill_fees','filled_size']
            for itemfloat in floatlist:
                rjson[itemfloat] = float(rjson.get(itemfloat, 0))
            rjson.pop('id', None)   # maybe delete
            db.insert_one(rjson)
            print('-DAXY OP {}'.format(kwargs["side"]))
            return rjson
        except KeyError:
            print("-DAXY OP KeyError..")
            db.insert_one({'MONGOKEY'   :   'ERROR_LOG',
                           'ERROR_MSG'  :   rjson,
                           'timestamp'  :   time.time()})

class mongowatcher():
    def __init__(self):
        self.client         =   GAC(key=key,b64secret=b64secret,
                                    passphrase=passphrase,
                                    api_url="https://api.pro.coinbase.com")
    
        self.op_            =   orderpicker(self.client)        
        self.hbcounter      =   time.time()
        self.ordertimer     =   self.hbcounter        
        self.order_interval =   100 #set to 100 for production
        self._update        =   {'BTC-USD'        : {'MARKET_UPDATE':False,'FBP_UPDATE':False},
                                 'ETH-USD'        : {'MARKET_UPDATE':False,'FBP_UPDATE':False}}
        self.UPDATE_HANDLER =   {'FBP_UPDATE'     : self.op_.storefbp,
                                 'MARKET_UPDATE'  : self.op_.storemarket,
                                 'None'           : self.error_pass}

    def error_pass(self):
        print('DAXY ERROR_PASS')

    def caller(self):
        NOW                         =   time.time()
        if NOW - self.hbcounter     >=  100:
            if random.random()      >   self.op_.cm_.longshort_adj:
                order_side          =   "buy"
            else:
                order_side          =   "sell"

            self.op_.cm_.heartbeat(NOW=NOW, order_side=order_side)  # __HB_order_canceller
            self.hbcounter          =   NOW

        if all(self._update.values()) is True and NOW - self.ordertimer > self.order_interval:
            if random.random()      >   0.3:
                print('+DAXY ORDER LOOP')
                random_order        =   np.random.random(2)

                if random_order[0]  >   self.op_.cm_.longshort_adj:
                    random_side     =   'buy'
                else:
                    random_side     =   'sell'

                if random_order[1]  <   0.65:
                    random_product  =   'BTC-USD'
                else:
                    random_product  =   'ETH-USD' 
                                
                flag, TK            =   self.op_.makeorder(side=random_side, 
                                                           product_id=random_product)   # __order_func_handler

                if flag is True:
                    self.client.ORDER(price=TK['price'], side=TK['side'], 
                                      size=TK['size'], product_id=TK['product_id']) # __order_func_executer
                else:
                    pass

                self.ordertimer     =   NOW
            else:
                self.ordertimer     =   NOW

            self.order_interval     =   8

        else:
            pass

    def watcher(self):
        try:
            with db.watch(
                    [{'$match':{'operationType': "insert"}},{'$replaceRoot':{'newRoot':'$fullDocument'}},
                     {'$match':{'MONGOKEY':{'$in': ['FBP_UPDATE', 'MARKET_UPDATE']}}}]) as stream:
                for insert_change in stream:
                    change_type     =   insert_change.get('MONGOKEY', 'None')
                    product_id      =   insert_change.get('product_id', 'None')
                    _update         =   self.UPDATE_HANDLER[change_type](insert_change) # __update_func 
                    self._update.update({product_id:{change_type:_update}})             
        except pymongo.errors.PyMongoError:
            print('DAXY MONGO WATCH ERROR')
            pass

    def caller_wrapper(self):
        exchange_update_counter = 0
        while True:
            self.caller()
            time.sleep(1)

            exchange_update_counter += 1
            if exchange_update_counter > 600:   # new exchange rate every 10 minutes
                self.op_.cm_.getexchangerate()  # __update_func_FOREX
                exchange_update_counter = 0
                try:
                    db.insert_one({'MONGOKEY'   :   'BALANCE',
                                   'timestamp'  :   time.time(),
                                   'product_id' :   'EUR-EUR',
                                   'balance'    :   self.op_.cm_.balance_longshort})    # __update_func_BALANCE
                except Exception:
                    pass

if __name__ == "__main__":
    mw_ = mongowatcher()
    print(mw_.op_.cm_.exchangerate)    

    T1 = Thread(target = mw_.watcher)
    T2 = Thread(target = mw_.caller_wrapper)

    T2.start()
    T1.start()