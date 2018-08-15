import websocket
try:
    import thread
except ImportError:
    import _thread as thread
import time
from pymongo import MongoClient
import datetime
import api

api = api.api()
mongo_client = MongoClient(api.mongo) #change for mongo

# specify the database and collection
db = mongo_client.test #change for mongo

def on_message(ws, message):
    msg = message
    OT = msg.get('order_type', None)

        if OT == 'market':
            current_time = time.time() 
            msg['funds'] = float(msg.get('funds', 0))
            msg['size'] = float(msg.get('size',0))
            if msg['size']  > 0 and msg['funds'] > 0:
                msg['y'] = msg['funds'] / msg['size']

                if msg['y'] > 0:
                    print(msg['y'])
                    if msg['product_id'] == 'BTC-USD':
                        mongo_collection = db.btcusd
                    elif msg['product_id'] == 'ETH-USD':
                        mongo_collection = db.ethusd
                    elif msg['product_id'] == 'LTC-USD':
                        mongo_collection = db.ltcusd

                    msg['_id'] = msg['order_id']
                    for popcolumn in self.popcolumns:
                        msg.pop(popcolumn, None)

                    msg['sequence'] = int(msg['sequence'])
                    msg['timestamp'] = time.time()  
                    msg['MONGOKEY'] = 'MARKET_UPDATE' 
                    try:
                        mongo_collection.insert_one(msg)
                    except Exception:
                        print('exception in parsing message to mongodb')

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    def run(*args):
        for i in range(3):
            time.sleep(1)
            ws.send("Hello %d" % i)
        time.sleep(1)
        ws.close()
        print("thread terminating...")
    thread.start_new_thread(run, ())


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws-feed.gdax.com/",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()