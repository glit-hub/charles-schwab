import pandas as pd
import os as os
from sqlalchemy import (create_engine, text, Column, Date, Integer, Numeric, MetaData, Table, Boolean, VARCHAR, UniqueConstraint)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from dotenv import load_dotenv
from access_token import access_tokens
import holidays
import pandas_market_calendars as mcal

load_dotenv(".env")
#region Get database info for API
user = os.environ["user"]
password = os.environ["password"]
database = os.environ["database"]
host = os.environ["host"]
port = os.environ["port"]

#endregion

# region - connect to postgres database
engine = create_engine(
    (
        f"postgresql://{user}:"
        f"{password}"
        f"@{host}:{port}/{database}"
    )
)

connection = engine.connect()
# endregion

# region - create tables
metadata = MetaData()

stock_info = Table(
    "stock_info",
    metadata,
    Column("ticker", VARCHAR, nullable=False),
    Column("name", VARCHAR, nullable=True),
    Column("sector", VARCHAR, nullable=True),
    Column("industry", VARCHAR, nullable=True),
    Column("sp500", Boolean, nullable=True),
    Column("etf", Boolean, nullable=True)
)

stock_price = Table(
    "stock_price",
    metadata,
    Column("ticker", VARCHAR, nullable=False),
    Column("date", Date, nullable=False),
    Column("open", Numeric(12, 4), nullable=True),
    Column("close", Numeric(12, 4), nullable=True),
    UniqueConstraint('ticker', 'date', name='uq_stock_price_ticker_date')
)

ops_tracker = Table(
    'ops_tracker',
    metadata,
    Column('primary_key', Integer, primary_key=True, autoincrement=True),
    Column('tracking_key', Integer, nullable=False),
    Column('start_time', TIMESTAMP, nullable=False),
    Column('end_time', TIMESTAMP, nullable=False),
    Column('ops_key', Integer, nullable=False)
)

ops_name = Table(
    'ops_name',
    metadata,
    Column('ops_key', Integer, primary_key=True, nullable=False),
    Column('ops_name', VARCHAR, nullable=False),
    Column('ops_detail', VARCHAR, nullable=False),
    Column('effective_start_date', Date, nullable=False),
    Column('effective_end_date', Date)
)

metadata.create_all(engine)
# endregion

# region - establish operation names
data_ops_names = pd.DataFrame(
    {
        'ops_key': [1, 2, 3, 4, 5],
        'ops_name': ['get_new_stocks',
                    'add_price_history',
                    'check_yesterday',
                    'get_all_stocks', 
                    'add_latest_price'],
        'ops_detail': ['stocks in stock_info table but not stock_price table',
                        'establish prices for new stocks back in time',
                        'check if yesterady was a trading day',
                        'get every stock in stock_info table',
                        'append the latest stock price for all the stocks in stock_info'],
        'effective_start_date': ['2025-08-21', '2025-08-21', '2025-08-24', '2025-08-21', '2025-08-21'],
        'effective_end_date': ['9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31', '9999-12-31']
    }
)

data_ops_names.to_sql(
    name='ops_name',
    con=connection,
    if_exists='append',
    index=False
)
connection.commit()
# endregion

# region - create a dim date table for operations
start = '2021-01-01'
end = '2030-01-01'

rng = pd.date_range(start, end, freq='D')
df = pd.DataFrame({'date': rng})
df['day_of_week'] = df['date'].dt.day_name()
df['is_weekday']  = ~df['date'].dt.weekday.isin([5, 6])
years = sorted({d.year for d in rng})
us_holidays = holidays.UnitedStates(years=years, observed=True)
df['is_holiday']  = df['date'].dt.date.map(lambda d: d in us_holidays)
df['holiday_name'] = df['date'].dt.date.map(lambda d: us_holidays.get(d, None))
nyse = mcal.get_calendar('NYSE')
sched = nyse.schedule(start_date=start, end_date=end)
trading_days = pd.DatetimeIndex(sched.index.tz_localize(None))
df['is_trading_day'] = df['date'].isin(trading_days)

df.to_sql('dim_date', engine, if_exists='replace', index=False)
connection.commit()
# endregion
