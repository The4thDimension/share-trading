import yfinance as yf
import pandas as pd
import os

def fetch_share_data(shareName):
    stock = yf.Ticker(shareName)
    hist = stock.history(period="1d", interval="5m")
    return hist

def get_share_names_from_csv(file_path):
    df = pd.read_csv(file_path)
    share_names = df.iloc[:, 2].dropna().tolist()  # Second column (index 1)
    return share_names

csv_path = "data/shares.csv"  # Replace with your actual file path
shareNames = get_share_names_from_csv(csv_path)

for name in shareNames:
    try:
        val = fetch_share_data(name)
        if not val.empty:
            output_file = os.path.join("data/", f"{name.replace('.NS', '')}_5min.csv")
            val.to_csv(output_file)
            print(f"Saved: {output_file}")
        else:
            print(f"No data found for {name}")
    except Exception as e:
        print(f"Error fetching data for {name}: {e}")


