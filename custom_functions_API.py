import datetime as dt
import sharedVars as sv
import os as os
import pandas as pd
import requests
import time
from access_token import access_tokens
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
import pytz

load_dotenv(".env")
app_key = os.getenv('app_key')
app_secret = os.getenv('app_secret')

sv.at = access_tokens()

# region - check access token timer
def access_token_refresh(start_time):
    curr_time = dt.datetime.now()
    if curr_time - start_time > dt.timedelta(minutes=5):
        sv.at = access_tokens()
        start_time = dt.datetime.now()
# end region

def get_stock_now(ticker):
    params = {
        "symbol": ticker,
        "periodType": "month",
        "period": 1,
        "frequencyType": "daily",
        "frequency": 1,
        "needExtendedHoursData": "false"
    }
    headers = {"Authorization": f"Bearer {sv.at}"}
    response = requests.get(f"https://api.schwabapi.com/marketdata/v1/pricehistory",
                            params=params,
                            headers=headers)
    if response.status_code == 200:
        response = response.json()
        daily_data = pd.DataFrame(response['candles'])
        daily_data = daily_data.sort_values('datetime', ascending=False)
        data_yesterday = daily_data.head(1)
        data_yesterday = data_yesterday[['open', 'close', 'datetime']]
        data_yesterday['date'] = pd.to_datetime(data_yesterday['datetime'], unit='ms').dt.date
        data_yesterday['ticker'] = [ticker]

        get_stock_price = data_yesterday.copy()
        return get_stock_price

    else:
        print("Fix API endpoint!")


def get_stock_history(ticker):
    params = {
        "symbol": ticker,
        "periodType": "year",
        "period": 10,
        "frequencyType": "daily",
        "frequency": 1,
        "needExtendedHoursData": "false"
    }
    headers = {"Authorization": f"Bearer {sv.at}"}
    response = requests.get(f"https://api.schwabapi.com/marketdata/v1/pricehistory",
                            params=params,
                            headers=headers)
    if response.status_code == 200:
        response = response.json()
        df = pd.DataFrame(response['candles']).sort_values('datetime')
        df['date'] = pd.to_datetime(df['datetime'], unit='ms')
        et_today = dt.datetime.now(ZoneInfo("America/New_York")).date()
        yesterday_et = et_today - dt.timedelta(days=1)
        df = df[(df['date'] > '2023-01-01') & (df["date"] != yesterday_et)]
        df['date'] = df['date'].dt.date
        df['ticker'] = ticker
        return df
    else:
        print("Fix API endpoint!")