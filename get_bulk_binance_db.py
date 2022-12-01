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

ticker_list= ["BUSD","USDC","BNB","MATIC","LINK","ADA","DOT","AVAX","ETC","ATOM","NEAR","UNI","FIL",
             "AAVE","XMR","ALGO","XLM","CRV","GMX","AR","BAL","ROSE","DYDX","RUNE","MKR"]

month_list= ["01","02","03","04","05","06","07","08","09","10","11","12"]
testing_list= ["BTC","ETH"]
#params interval             
interval= "1m"
start_date = None
end_date= None



def get_bulk_crypto_data():


    #joining ticker with USDT to get final url for df
    for ticker in ticker_list: 
        symbol= ticker+ "USDT"
        #going through the year I like 2019 as mkt has changed since 2018 this for loop is brute force btw 
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
                    df = pd.read_csv(wget.download(url),compression='zip',names=columns)
                    df= df.drop("Ignore",axis="columns")
                    df["ticker"]= ticker


                    #change unix to UTC human time  
                    df ['date']= pd.to_datetime(df['date'], unit='ms',origin= 'unix')
                    df['Close time']= pd.to_datetime(df['date'], unit='ms',origin= 'unix')


                    #give it to the sql db 
                    df.to_sql("Binance_USDT_DB",
                        conn, 
                        if_exists="replace",
                        index=False)
                    time.sleep(2)

                    #download percentage to be added later       

                #if response is bad lets try the next 
                else: 
                    time.sleep(1)
                    continue
                    





print(get_bulk_crypto_data())

