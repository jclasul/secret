from pymongo import MongoClient
import datetime
import time
import gdax
import api_keys
import json

api = api_keys.api_keys()
mongo_client = MongoClient(api.mongo) #change for mongo
print(api.mongo)

# specify the database and collection
db = mongo_client.test #change for mongo

class myWebsocketClient(gdax.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com"
        self.products = ["BTC-USD"] #,"ETH-USD"]  
        self.channels = ["full"] #,"full"]      
        self.api_key = api.api_key
        self.api_secret = api.secret_key
        self.api_passphrase = api.passphrase
        print(api.api_key)
    def _listen(self):
        start_t = time.time()
        while not self.stop:
            try:              
                if time.time() - start_t >= 30:
                    # Set a 30 second ping to keep connection alive
                    print('pinging _listen')
                    self.ws.ping("keepalive")
                    start_t = time.time()
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                print('ValueError _listen', e)
                self.on_error(e)
                self._disconnect()
            except Exception as e:
                print('Exception _listen',e)
                self.on_error(e)
                self._disconnect()
            else:
                self.on_message(msg)

    def on_message(self, msg):
        OT = msg.get('order_type', None)

        if OT == 'market':
            current_time = time.time() 
            msg['funds'] = float(msg.get('funds', 0))
            msg['size'] = float(msg.get('size',0))
            if msg['size']  > 0 and msg['funds'] > 0:
                msg['y'] = msg['funds'] / msg['size']

                if msg['y'] > 0:
                    print(msg.get('y', 'no calc price'),
                            msg.get('product_id', 'no product id'))
                    mongo_collection = db.btcusd

                    msg['_id'] = msg['order_id']
                    popcolumns = ['order_id','client_oid','price']
                    for popcolumn in popcolumns:
                        msg.pop(popcolumn, None)

                    msg['sequence'] = int(msg['sequence'])
                    msg['timestamp'] = time.time()  
                    msg['MONGOKEY'] = 'MARKET_UPDATE' 
                    try:
                        mongo_collection.insert_one(msg)
                    except Exception:
                        print('exception in parsing message to mongodb')
                        print(msg)
  
    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def on_close(self):
        self.ws.close()
        print('closed')
        time.sleep(30)
        wsClient = myWebsocketClient()
        wsClient.start()
        print('restarted after failure')   

    def on_error(self, e, data=None):
        self.error = e
        print(self.error)
        print('on_error handler')
        time.sleep(30)
        self._disconnect()

if __name__ == "__main__": 
    wsClient = myWebsocketClient()        
    wsClient.start()
    print(wsClient.url, wsClient.products)