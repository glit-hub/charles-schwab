# region - load packages 
import requests
from loguru import logger
import os
import pandas as pd
import time
import datetime as dt
from access_token import access_tokens
import sharedVars as sv
import customFunctionsGeneral as cfg
import custom_functions_API as cfa
from dotenv import load_dotenv
import sqlalchemy
import pytz
# endregion

# region - establish connection to postgres database
cfg.write_log('File Initiation. I am about to run Postgres connection check.')
cfg.initiatePostgresConnection()
cfg.checkIfPostgresConnectionWorks()
cfg.write_log('Successfully connected to Postgres database')
# endregion

# region - get new tracking key
max_tracking_key = cfg.getMaxTrackingKey()
# endregion

# region - set access token timer
start_time = dt.datetime.now()
# endregion

# region - get all stocks that we do not have data of
cfg.write_log("I am getting all the new stocks.")
zRetrieveNewStocks = {
    'start_time': dt.datetime.now(),
    'ops_key': 1
}

new_stocks_df = cfg.retrieve_new_stocks()

zRetrieveNewStocks['end_time'] = dt.datetime.now()
cfg.upload_ops_time(max_tracking_key=max_tracking_key,
                    ops_key=zRetrieveNewStocks['ops_key'],
                    start_time=zRetrieveNewStocks['start_time'],
                    end_time=zRetrieveNewStocks['end_time'])

# region -check time
cfa.access_token_refresh(start_time)

zAddPriceHistory = {
    'ops_key': 2,
    'start_time': dt.datetime.now()
}
if len(new_stocks_df) > 0:
    for stock in new_stocks_df['ticker']:
        cfa.access_token_refresh(start_time)

        cfg.write_log(f"Getting historical price for {stock}")
        stock_history = cfa.get_stock_history(stock)
        cfg.add_stock_price_hist(stock_history)
        cfg.write_log(f"I got past prices for {stock} except for {dt.date.today()}")
        curr_time = dt.datetime.now()

else:
    cfg.write_log('No new stocks for today.')

zAddPriceHistory['end_time'] = dt.datetime.now()
cfg.upload_ops_time(max_tracking_key=max_tracking_key,
                    ops_key=zAddPriceHistory['ops_key'],
                    start_time=zAddPriceHistory['start_time'],
                    end_time=zAddPriceHistory['end_time'])
# endregion


# region - check if yesterday was a holiday
zCheckYesterday = {
    'ops_key': 3,
    'start_time': dt.datetime.now()
}
cfg.write_log("Checking if yesterday was a trading day.")
yesterday_trading_day = cfg.yesterday_is_holiday()

zCheckYesterday['end_time'] = dt.datetime.now()
cfg.upload_ops_time(max_tracking_key=max_tracking_key,
                    ops_key=zCheckYesterday['ops_key'],
                    start_time=zCheckYesterday['start_time'],
                    end_time=zCheckYesterday['end_time'])
# endregion

# region - get the price of each stock of yesterday
if yesterday_trading_day == True:
    cfg.write_log("I am getting every single ticker in the stock_info table")
    zGetAllStocks = {
        'ops_key': 4,
        'start_time': dt.datetime.now()
    }
    all_stocks_df = cfg.retrieve_all_stocks()

    zGetAllStocks['end_time'] = dt.datetime.now()
    cfg.upload_ops_time(
        max_tracking_key=max_tracking_key,
        ops_key=zGetAllStocks['ops_key'],
        start_time=zGetAllStocks['start_time'],
        end_time=zGetAllStocks['end_time']
    )

    zUpdateStockPrice = {
        'ops_key': 5,
        'start_time': dt.datetime.now()
    }
    for ticker in all_stocks_df['ticker']:
        cfa.access_token_refresh(start_time)
        stock_price_df = cfa.get_stock_now(ticker)
        cfg.add_stock_price(stock_price_df)
    cfg.write_log(f"I have added the stock prices for yesterday ({stock_price_df['date']})")
    zUpdateStockPrice['end_time'] = dt.datetime.now()
    cfg.upload_ops_time(
        max_tracking_key=max_tracking_key,
        ops_key=zUpdateStockPrice['ops_key'],
        start_time=zUpdateStockPrice['start_time'],
        end_time=zUpdateStockPrice['end_time']
    )
# if not a trading day, then ops_key 4 and 5 have negligable duration
else:
    zGetAllStocks = {
        'ops_key': 4,
        'start_time': dt.datetime.now()
    }
    zGetAllStocks['end_time'] = dt.datetime.now()
    cfg.upload_ops_time(
        max_tracking_key=max_tracking_key,
        ops_key=zGetAllStocks['ops_key'],
        start_time=zGetAllStocks['start_time'],
        end_time=zGetAllStocks['end_time']
    )
    zUpdateStockPrice = {
        'ops_key': 5,
        'start_time': dt.datetime.now()
    }
    zUpdateStockPrice['end_time'] = dt.datetime.now()
    cfg.upload_ops_time(
        max_tracking_key=max_tracking_key,
        ops_key=zUpdateStockPrice['ops_key'],
        start_time=zUpdateStockPrice['start_time'],
        end_time=zUpdateStockPrice['end_time']
    )

# endregion


