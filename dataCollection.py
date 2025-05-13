import yfinance as yf
import pandas as pd
import os
import sqlite3
from datetime import datetime
import time
import logging
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
import schedule
import time
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
import random
import mplfinance as mpf
import pandas as pd
import sqlite3
from ta.trend import SMAIndicator
import plotly.graph_objects as go

# ========== Clean Data ==========
def clean_stock_data(df):
    df.drop_duplicates(inplace=True)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.sort_values(by='datetime', inplace=True)
    df.interpolate(method='linear', inplace=True)
    # Clip values beyond 3 standard deviations
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].clip(lower=df[col].mean() - 3*df[col].std(), upper=df[col].mean() + 3*df[col].std())
    df[['open', 'high', 'low', 'close', 'volume']] = scaler.fit_transform(df[['open', 'high', 'low', 'close', 'volume']])
    return df


# ========== Logging Setup ==========
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/fetch_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========== Telegram Alert Setup ==========
# Load from .env file
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }
        requests.post(url, data=payload)
    except Exception as e:
        logging.error(f"Failed to send Telegram alert: {e}")

# ========== Fetch Share Data ==========
def fetch_share_data(shareName, retries=3, delay=5):
    for attempt in range(retries):
        try:
            stock = yf.Ticker(shareName)
            hist = stock.history(period="1d", interval="5m")
            hist = hist.reset_index()
            hist['symbol'] = shareName
            hist['fetched_at'] = datetime.now()
            hist.rename(columns={
                'Datetime': 'datetime',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume',
                'Dividends': 'dividends',
                'Stock Splits': 'stock_splits'
            }, inplace=True)
            return hist
        except Exception as e:
            logging.warning(f"Retry {attempt + 1} failed for {shareName}: {e}")
            time.sleep(delay)
    raise Exception(f"All {retries} attempts failed for {shareName}")

# ========== Get Share Names ==========
def get_share_names_from_csv(file_path):
    df = pd.read_csv(file_path)
    return df.iloc[:, 2].dropna().tolist()

# ========== Main Job ==========
def job():
    try:
        csv_path = "data/shares.csv"
        db_path = "data/stock_data.db"
        table_name = "stock_prices"

        shareNames = get_share_names_from_csv(csv_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        df = pd.read_sql_query("SELECT * FROM stock_prices WHERE symbol='TCS.NS' ORDER BY datetime DESC LIMIT 100", conn)

        # # Prepare DataFrame for mplfinance
        # df['datetime'] = pd.to_datetime(df['datetime'])
        # df.set_index('datetime', inplace=True)
        # df = df[['open', 'high', 'low', 'close', 'volume']]
        # df.sort_index(inplace=True)

        # # Plot candlestick chart
        # mpf.plot(df, type='candle', volume=True, title='TCS.NS Candlestick Chart', style='yahoo')

        
        # Simple Moving Average
        sma = SMAIndicator(close=df['close'], window=14)
        df['SMA14'] = sma.sma_indicator()

        df['datetime'] = pd.to_datetime(df['datetime'])  # Step 1: ensure it's datetime type
        df.set_index('datetime', inplace=True)           # Step 2: set it as index
        df.sort_index(inplace=True)                      # Step 3 (optional): make sure it's sorted

        # Plot with SMA
        mpf.plot(df, type='candle', volume=True, style='yahoo', addplot=mpf.make_addplot(df['SMA14'], color='blue'), title='TCS.NS with 14-day SMA')

        # Plot with plotly
        # fig = go.Figure(data=[go.Candlestick(
        # x=df.index,
        # open=df['open'],
        # high=df['high'],
        # low=df['low'],
        # close=df['close']
        # )])

        # fig.update_layout(title='TCS.NS Candlestick Chart (Interactive)', xaxis_title='Date', yaxis_title='Price')
        # fig.show()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            datetime TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            dividends REAL,
            stock_splits REAL,
            symbol TEXT,
            fetched_at TEXT,
            PRIMARY KEY (datetime, symbol)
        )""")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_symbol ON {table_name}(symbol)")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_datetime ON {table_name}(datetime)")
        conn.commit()

        for name in shareNames:
            try:
                data = fetch_share_data(name)
                if not data.empty:
                    data = clean_stock_data(data)  # 🧹 Clean the data
                    if not data.empty:  # Recheck if it's still not empty after cleaning
                        data.to_sql(table_name, conn, if_exists="append", index=False)
                        logging.info(f"Appended cleaned data for {name}")
                    else:
                        logging.info(f"Cleaned data is empty for {name}")
                else:
                    logging.info(f"No data returned for {name}")
                # Add delay
                time.sleep(random.uniform(1, 3))
            except Exception as e:
                error_msg = f"Error fetching data for {name}: {e}"
                logging.error(error_msg)
                send_telegram_alert(error_msg)

        conn.commit()
        conn.close()
        logging.info("Job completed successfully")
        print('Job completed successfully')

    except Exception as e:
        crash_msg = f"Scheduler crashed: {e}"
        logging.critical(crash_msg)
        send_telegram_alert(crash_msg)

# schedule.every().hour.do(job)

# while True:
#     schedule.run_pending()
#     time.sleep(1)

job()