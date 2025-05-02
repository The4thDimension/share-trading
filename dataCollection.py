import yfinance as yf
import pandas as pd
import os
import sqlite3
from datetime import datetime

def fetch_share_data(shareName):
    stock = yf.Ticker(shareName)
    hist = stock.history(period="1d", interval="5m")
    hist = hist.reset_index()
    hist['symbol'] = shareName
    hist['fetched_at'] = datetime.now()  # Optional audit column

    # Rename columns to match SQLite schema
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

def get_share_names_from_csv(file_path):
    df = pd.read_csv(file_path)
    share_names = df.iloc[:, 2].dropna().tolist()  # Second column (index 2)
    return share_names

# File paths
csv_path = "data/shares.csv"
db_path = "data/stock_data.db"
table_name = "stock_prices"

# Fetch and store data
shareNames = get_share_names_from_csv(csv_path)

# SQLite connection
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table with primary key and index (if not exists)
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
)
""")

cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_symbol ON {table_name}(symbol)")
cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_datetime ON {table_name}(datetime)")
conn.commit()

for name in shareNames:
    try:
        val = fetch_share_data(name)
        if not val.empty:
            val.to_sql(table_name, conn, if_exists="append", index=False)
            print(f"Appended: {name}")
        else:
            print(f"No data found for {name}")
    except Exception as e:
        print(f"Error fetching data for {name}: {e}")

conn.commit()
conn.close()
