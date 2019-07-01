import requests
import pandas as pd
import json
import datetime;
import subprocess
import os
import time
import logging


logging.basicConfig(filename='log.log',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

tick_time = 5 #seconds


while True:

    timestamp = datetime.datetime.now().timestamp()
    timestamp = datetime.datetime.fromtimestamp(timestamp).isoformat()

    volume = requests.get('https://www.binance.com/exchange/public/product')
    price = requests.get('https://www.binance.com/api/v1/ticker/allBookTickers')
    while volume.status_code  != 200 or price.status_code != 200:
        if volume.status_code  != 200:
            logging.debug('The request for volume returned '+str(volume.status_code))
        if price.status_code  != 200:
            logging.debug('The request for price returned '+str(price.status_code))
        timestamp = datetime.datetime.now().timestamp()
        timestamp = datetime.datetime.fromtimestamp(timestamp).isoformat()
        volume = requests.get('https://www.binance.com/exchange/public/product')
        price = requests.get('https://www.binance.com/api/v1/ticker/allBookTickers')
        time.sleep(10)

    #The volume request returns a json with a single line 'data' which is a dictionary
    #Then I convert this dictionary to a dataframe
    volume = volume.json()['data']
    volume = pd.DataFrame(volume)
    volume['bidPrice']=-1
    volume['bidQty']=-1
    volume['askPrice']=-1
    volume['askQty']=-1

    price = price.json()
    price = pd.DataFrame(price)

    for pair in volume['symbol']:
            try: #There are some pairs thar aren't being shown on  https://www.binance.com/api/v1/ticker/allBookTickers. GTOPAX is one of them at this time
                line = volume[volume.symbol==pair]
                line['bidPrice'] = price[price.symbol==pair]['bidPrice'].iat[0]
                line['bidQty'] = price[price.symbol==pair]['bidQty'].iat[0]
                line['askPrice'] = price[price.symbol==pair]['askPrice'].iat[0]
                line['askQty'] = price[price.symbol==pair]['askQty'].iat[0]
            except:
                logging.warning('No matching "price" pair on the allBookTirckers endpoint. '+pair+'. Using the -1 flag instead')            
            
            line['timestamp'] = timestamp    
            file_path = '../dumps/'+pair+'.csv'
            mode = 'w+'
            use_header=True
            if os.path.isfile(file_path):
                mode = 'a+'
                use_header=False
            f = open(file_path,mode)
            f.write(line.to_csv(index=False,header=use_header))
            f.close()

    logging.info('Syncing on Git')
    #subprocess.run("git add ../dumps/*",shell=True)
    #subprocess.run("git pull",shell=True) #This pull will be useful when we have more than one server runing
    #subprocess.run("git commit -m "+'"'+timestamp+'"',shell=True)
    #subprocess.run("git push",shell=True)
    logging.info('Commit '+timestamp+' pushed to git')
  
    logging.info('Sleeping for '+str(tick_time/60)+' minute(s)')
    timestamp_now = datetime.datetime.now().timestamp()
    timestamp_now = datetime.datetime.fromtimestamp(timestamp_now).isoformat()   
    logging.info('Sleeping for'+str(tick_time)+' seconds')
    time.sleep(tick_time)
