import pandas as pd
import numpy as np
from fbprophet import Prophet
from pymongo import MongoClient
import pymongo
import time
import gdax
    
import matplotlib.pyplot as plt

client = pymongo.MongoClient(
    'mongodb+srv://admin:jan@gdaxcluster-fn53m.gcp.mongodb.net/test?retryWrites=true')
# specify the database and collection`
db = client.gdax.gdaxws
g = gdax.PublicClient()

def balanceplot(dfm):
    generated_lower = time.time()
    time_delta = 3600*24*5

    placeholder_lower_int = generated_lower

    placeholder_greater_int = placeholder_lower_int - time_delta


    df = pd.DataFrame(list(db.find({'MONGOKEY':'BALANCE',
                                    'timestamp' : {'$gt':placeholder_greater_int}})\
                        .sort([('timestamp', 1)]))) 
    df['time'] = pd.to_datetime(df['timestamp'],unit='s')
    df.set_index('time',drop=False, inplace=True)
    df = df.resample('5min').mean()
    fig, ax1 = plt.subplots(facecolor='black', figsize=(20,12))
    ax1.plot(df.index, df['balance'])
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:blue'
    ax2.set_ylabel('BTC-USD', color='w')  # we already handled the x-label with ax1
    ax2.plot(dfm.index, dfm['y'], color=color, alpha = 0.4)
    ax2.tick_params(axis='y', labelcolor='w')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    tt = time.ctime()
    plt.savefig('static/{}_balance.png'.format(tt),dpi=100)
    print('saved')

def marketplot():
    generated_lower = time.time()
    time_delta = 3600*24*5

    placeholder_lower_int = generated_lower

    placeholder_greater_int = placeholder_lower_int - time_delta

    df = pd.DataFrame(list(db.find({'MONGOKEY':'MARKET_UPDATE',
                                    'product_id':'BTC-USD',
                                    'timestamp' : {'$gt':placeholder_greater_int}})\
                        .sort([('timestamp', 1)]))) 

    df_fbp = pd.DataFrame(list(db.find({'MONGOKEY' : 'FBP_UPDATE','product_id':'BTC-USD',
                                        'timestamp' : {'$gt':placeholder_greater_int}})\
                            .sort([('timestamp', 1)]))) 

    marketBTCUSD = g.get_product_ticker("BTC-USD")
    marketBTCEUR = g.get_product_ticker("BTC-EUR")
    exchangerate = float(marketBTCEUR["price"]) / float(marketBTCUSD["price"])
    print('USD EUR at : ', exchangerate)

    df['time'] = pd.to_datetime(df['time'],infer_datetime_format=True)
    df.rename(columns={'y':'y','time':'ds'}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') 
    df['delta'] = (df['y'] - df['y'].shift(1))
    df['rolling_delta'] = (df['delta'].rolling(20).mean()) # 20 ~ 4 minutes
    df['sqr_rolling_delta'] = np.square(df['rolling_delta'])
    df.set_index('ds',drop=False, inplace=True)
    df_fbp.set_index('ds_fcst_0002',drop=False, inplace=True)

    df = df.resample('5min').mean()
    df_fbp = df_fbp.resample('5min').mean()

    dff = df
    df_m = pd.merge(df,df_fbp, left_index=True, right_index=True, how='outer')

    fcu = 'yhat_lower_fcst_002'
    fcl = 'yhat_upper_fcst_002'
    fcu_2 = 'yhat_lower_fcst_0002'
    fcl_2 = 'yhat_upper_fcst_0002'

    plt.style.use('dark_background')
    fig, ax1 = plt.subplots(facecolor='black', figsize=(20,12))

    ax1.plot(df_m.index,df_m['y'], alpha=0.5)
    ax1.plot(df_m.index,df_m['trend_fcst_0002'], alpha =0.3)
    ax1.plot(df_m.index,df_m['trend_fcst_002'], alpha =0.3)
    ax1.fill_between(df_m.index, df_m[fcl], 
                    df_m[fcu],color='#FFFF00', alpha=0.2)
    ax1.fill_between(df_m.index, df_m[fcl_2],
                    df_m[fcu_2],color='#0000ff', alpha=0.3)

    ax1.set_ylabel('price in USD', color='w')
    plt.xlabel('date')

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    tt = time.ctime()
    plt.savefig('static/{}_market.png'.format(tt),dpi=100)
    print('saved')

    return df

if __name__ == '__main__':
    dfm = marketplot()
    balanceplot(dfm=dfm)