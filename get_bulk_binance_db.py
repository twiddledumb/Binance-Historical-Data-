import requests
import pandas as pd 
import json  
import wget
import sqlite3
from datetime import *
import time

# sqlite con 
conn= sqlite3.connect("Binance_USDT_DB")

#here is your ticket list. 
ticker_list= ["copy format of testing_list and put your tickers into here"]
testing_list= ["BTC","ETH"]


month_list= ["01","02","03","04","05","06","07","08","09","10","11","12"]


#params interval             
interval= "1d" # intervals are ..12h,15m,1d,1h,1m,1mo,1s,1w,2h,30m,3d,3m,4h,5m,6h,8h
start_date = None
end_date= None



def get_bulk_crypto_data():


    #joining ticker with USDT to get final url for df
    for ticker in ticker_list: 
        symbol= ticker+ "USDT" #if you would like another denominator ie USDC OR BUSD replace it here
        
        #going through the year I like 2019 as mkt has changed since 2018,change 2019 to whatever year you seemed please to but most tickers start in 2020-01 fyi
        for year in range(2019,int(datetime.now().year+1)):
            for month in month_list:
                
                year= str(year)
                month= str(month)
                #get url to see response either 404 or we are all good 
                url2= "https://data.binance.vision/data/spot/monthly/klines/"+symbol+"/"+interval+"/"+symbol+"-"+interval+"-"+year+"-"+month+".zip"
                response = requests.get(url2)

                #if response is good let grab that file and put it into our db
                if str(response) != "<Response [404]>":
                    print(" Ticker: "+ ticker + " year: "+str(year) + " month: "+ str(month))
                    url= "https://data.binance.vision/data/spot/monthly/klines/"+symbol+"/"+interval+"/"+symbol+"-"+interval+"-"+year+"-"+month+".zip"

                    #' example url url= "https://data.binance.vision/data/spot/monthly/klines//1h/BTCUSDT-1h-2020-08.zip"



                    # df set up 
                    columns = ['date',"Open","High","Low","Close","Volume","Close time","Quote asset volume","Number of trades","Taker buy base asset volume","Taker buy quote asset volume","Ignore"] 
                    
                    #I did not organize where the files are downloaed as I already have the db. After the function is done feel free to copy and paste it all into one folder 
                    df = pd.read_csv(wget.download(url),compression='zip',names=columns)
                    
                    #we drop ignore since binance does not use this anymore
                    df= df.drop("Ignore",axis="columns")
                    df["ticker"]= ticker


                    #change unix to UTC   
                    df ['date']= pd.to_datetime(df['date'], unit='ms',origin= 'unix')
                    df['Close time']= pd.to_datetime(df['date'], unit='ms',origin= 'unix')


                    #give it to the sql db 
                    df.to_sql("Binance_USDT_DB",
                        conn, 
                        if_exists="replace",
                        index=False)
#timers are put in to be nice to the api since all the other exchanges make it more difficult to get this info. Pls continue to be nice to Binance                    
                    time.sleep(2)
     

                #if response is bad lets try the next 
                else: 
                    time.sleep(1)
                    continue
                    





print(get_bulk_crypto_data())

