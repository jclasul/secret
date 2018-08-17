from pymongo import MongoClient
import datetime
import time
import gdax
import api_keys
import json

api = api_keys.api_keys()
mongo_client = MongoClient(api.mongo) #change for mongo

# specify the database and collection
db = mongo_client.gdax.gdaxws #change for mongo

class myWebsocketClient(gdax.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com"
        self.products = ["BTC-USD","ETH-USD"] #,"ETH-USD"]  
        self.channels = ["full","full"] #,"full"]    

    def _listen(self):
        start_t = time.time()
        while not self.stop:
            try:              
                if time.time() - start_t >= 30:
                    # Set a 30 second ping to keep connection alive
                    print(time.ctime(),'WS pinging _listen')
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
        #print(OT) #debudding 

        if OT == 'market':
            current_time = time.time() 
            msg['funds'] = float(msg.get('funds', 0))
            msg['size'] = float(msg.get('size',0))
            if msg['size']  > 0 and msg['funds'] > 0:
                msg['y'] = msg['funds'] / msg['size']

                if msg['y'] > 0:
                    # print(msg.get('y', 'no calc price'),msg.get('product_id', 'no product id'))
                    msg['_id'] = msg['order_id']
                    popcolumns = ['order_id','client_oid','price']
                    for popcolumn in popcolumns:
                        msg.pop(popcolumn, None)

                    msg['sequence'] = int(msg['sequence'])
                    msg['timestamp'] = time.time()  
                    msg['MONGOKEY'] = 'MARKET_UPDATE' 
                    try:
                        db.insert_one(msg)
                        print(time.ctime(),'WS MONGO INSERT: {}'.format(msg.get('y', None)))
                    except Exception:
                        print(time.ctime(),'WS MONGO INSERT ERROR')
  
    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def on_close(self):
        self.ws.close()
        print('WS CLOSED')
        time.sleep(30)
        wsClient = myWebsocketClient()
        wsClient.start()
        print('WS RESTARTED after failure')   

    def on_error(self, e, data=None):
        self.error = e
        print(self.error)
        print('WS on_error handler')
        time.sleep(30)
        self._disconnect()

if __name__ == "__main__": 
    wsClient = myWebsocketClient()        
    wsClient.start()
    print(time.ctime(),'WS STARTED\n', wsClient.url, wsClient.products)