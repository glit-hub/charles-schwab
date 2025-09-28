import datetime as dt
import sharedVars as sv
from dotenv import load_dotenv
import os as os
from sqlalchemy import create_engine, text
import pandas as pd
import sys
from zoneinfo import ZoneInfo


# region - Write Log

def write_log(message=''):

    """ Retrieves message given as parameter and logs execution details and
    timestamp in file specified in sharedVars.py. If file hasn't been created,
    then create a new file designated to the current time of execution.
    """
    # Initiate a file name to write
    log_folder = 'docs'
    if sv.file_name_for_log is None:
        sv.file_name_for_log = dt.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

    # Check if a text is provided
    if message == '':
        print('Hey you need to provide a message to write in a log file')
    # If provided, we will write text message to the file
    else:
        file_path = os.path.join(log_folder, f'{sv.file_name_for_log}.txt')
        with open(file_path, 'a') as f:
            f.write('On ' + dt.datetime.now().strftime('%b %d, %Y') + ' at ' +
                    dt.datetime.now().strftime('%H:%M:%S') + ', '
                    + message + '\n')

# endregion - Write Log

# region - Initiate Postgres Connection
def initiatePostgresConnection():
    load_dotenv(".env")
    sv.user = os.environ['user']
    sv.password = os.environ['password']
    sv.database = os.environ['database']
    sv.host = os.environ['host']
    sv.port = os.environ['port']

# endregion - Initiate Postgres Connection


# region - Check Postgres Connection
def checkIfPostgresConnectionWorks():
    sv.engine = create_engine(
        (
            f"postgresql://{sv.user}:"
            f"{sv.password}"
            f"@{sv.host}:{sv.port}/{sv.database}"
        )
    )
    sv.connection = sv.engine.connect()

    sv.query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """

    try:
        pd.read_sql_query(sv.query, sv.connection)
        print('all good')

    except Exception as e:
        write_log("""I am in checkIfPostgresConnectionWorks exception.
                There is an issue with database connection""")
        sys.exit(1)

# endregion - Check Postgres Connection

# region - Get Max Tracking Key

def getMaxTrackingKey():

    sv.query = """
        SELECT tracking_key from ops_tracker;
    """

    tracking = pd.read_sql_query(sv.query, sv.connection)

    if len(tracking) == 0:
        max_tracking_key = 1
    else:
        max_tracking_key = tracking['tracking_key'].max()+1

    return max_tracking_key

# endregion - Get Max Tracking Key

# region - uploading operations and the durations
def upload_ops_time(max_tracking_key='', ops_key='', start_time='', end_time=''):
    zOpsTracking = pd.DataFrame(
        {
            'tracking_key': [max_tracking_key],
            'ops_key': [ops_key],
            'start_time': [start_time],
            'end_time': [end_time]
        }
    )
    zOpsTracking.to_sql(
        name='ops_tracker',
        con=sv.connection,
        if_exists='append',
        index=False
    )
    sv.connection.commit()
# endregion

# region - check yesterday is holiday
def yesterday_is_holiday():
    et_today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    yesterday_et = et_today - dt.timedelta(days=1)
    yesterday_str = yesterday_et.strftime("%Y-%m-%d")
    query = f"""SELECT DATE(date) AS date, is_trading_day FROM dim_date
            WHERE date = '{yesterday_str}'"""
    try:
        yesterday_info = pd.read_sql_query(
            query,
            con=sv.connection
        )
        if len(yesterday_info) != 1:
            write_log("Error in checking yesterday info.")
            sys.exit(1)
            
        is_trading = bool(yesterday_info["is_trading_day"].iloc[0])

        if not is_trading:
            write_log("Yesterday was not a trading day")
            return False
        else:
            write_log("Yesterday was a trading day. We can continue.")
            return True
    except Exception as e:
        write_log("Error in checking yesterday info.")
        sys.exit(1)
    

# region - get every stock we still want
def retrieve_all_stocks():
    query = "SELECT ticker FROM stock_info"

    try:
        all_stock_data = pd.read_sql_query(
            query,
            con=sv.connection
        )
    except Exception as e:
        write_log("Retrieval of all stock tickers failed.")
        sys.exit(1)
    return all_stock_data

# endregion

# region - Upload Ops Time

def uploadOpsTime(maxTrackingKey='', starttime='', endtime='', ops_key=''):
    zOpsTracking = pd.DataFrame(
        {
            'tracking_key': [maxTrackingKey],
            'start_time': [starttime],
            'end_time': [endtime],
            'ops_key': [ops_key]
        }
    )
    zOpsTracking.to_sql(
        name='ops_tracker',
        con=sv.connection,
        if_exists='append',
        index=False
    )
    sv.connection.commit()

# endregion - Upload Ops Time

# region - see the stocks we don't have price data
def retrieve_new_stocks():
    query = "SELECT si.ticker FROM stock_info si LEFT JOIN stock_price sp ON si.ticker = sp.ticker WHERE sp.ticker IS NULL"

    try:
        new_stock_data = pd.read_sql_query(
            query,
            con=sv.connection
        )
    except Exception as e:
        write_log("Retrieval of new stocks failed.")
        sys.exit(1)
    return new_stock_data

# endregion

# region - add stock price info for the day
def add_stock_price(new_stock_data: pd.DataFrame):
    new_stock_data = new_stock_data.copy()
    ticker = str(new_stock_data["ticker"].iloc[0])
    date_val = pd.to_datetime(new_stock_data["date"].iloc[0]).date()
    open_val = float(new_stock_data["open"].iloc[0])
    close_val = float(new_stock_data["close"].iloc[0])

    query = """INSERT INTO stock_price (ticker, date, open, close) VALUES (:ticker, :date, :open, :close)
            ON CONFLICT (ticker, date) DO UPDATE
            SET open = EXCLUDED.open,
                close = EXCLUDED.close"""
    try:
        with sv.engine.begin() as conn:
            conn.execute(text(query), {
                "ticker": ticker,
                "date": date_val,
                "open": open_val,
                "close": close_val
        })
        print(f"Successfully added price of {new_stock_data['ticker']} on {new_stock_data['date']} to stock_price table")
    except Exception as e:
        write_log(f"There is an issue with adding {new_stock_data['ticker']}")
        sys.exit(1)
# endregion

# region - for the past years add stock price info
def add_stock_price_hist(curr_stock_prices: pd.DataFrame):
    curr_stock_prices = curr_stock_prices.copy()
    if curr_stock_prices.empty:
        print("No rows to insert.")
        return
    curr_stock_prices['ticker'] = curr_stock_prices['ticker'].astype(str)
    curr_stock_prices['open'] = pd.to_numeric(curr_stock_prices['open'], errors='coerce')
    curr_stock_prices['close'] = pd.to_numeric(curr_stock_prices['close'], errors='coerce')
    curr_stock_prices = curr_stock_prices.dropna(subset=['ticker', 'date'])
    rows = curr_stock_prices[["ticker","date","open","close"]].to_dict("records")
    query = "INSERT INTO stock_price (ticker, date, open, close) VALUES (:ticker, :date, :open, :close)"
    try:
        with sv.engine.begin() as conn:
            conn.execute(text(query), rows)
        print(f"Inserted {curr_stock_prices['ticker']} history into stock_price ")
    except Exception as e:
        write_log(f"There is an issue with adding {curr_stock_prices['ticker']}")
        sys.exit(1)

# endregion
