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
            hist = stock.history(period="1h", interval="1m")
            hist = hist.reset_index()
            hist['symbol'] = shareName
            hist['fetched_at'] = datetime.now()
            hist.rename(columns={
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
                    data.to_sql(table_name, conn, if_exists="append", index=False)
                    logging.info(f"Appended data for {name}")
                else:
                    logging.info(f"No data returned for {name}")
            except Exception as e:
                error_msg = f"Error fetching data for {name}: {e}"
                logging.error(error_msg)
                send_telegram_alert(error_msg)

        conn.commit()
        conn.close()
        logging.info("Job completed successfully")

    except Exception as e:
        crash_msg = f"Scheduler crashed: {e}"
        logging.critical(crash_msg)
        send_telegram_alert(crash_msg)

schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)