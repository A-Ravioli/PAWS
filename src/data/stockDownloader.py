import yfinance as yf
import pandas as pd
from datetime import datetime
from tqdm import tqdm

def read_tickers_from_csv(file_path):
    # Read tickers from CSV file
    df = pd.read_csv(file_path)
    return df['Symbol'].tolist()

def download_stock_data(tickers, start_date, end_date):
    stock_data = pd.DataFrame()

    for ticker in tqdm(tickers, desc="Downloading Stock Data", unit="ticker"):
        # Download stock data
        data = yf.download(ticker, start=start_date, end=end_date)
        
        # Concatenate to the overall stock_data DataFrame
        stock_data = pd.concat([stock_data, data['Adj Close']], axis=1, sort=False)

    return stock_data

def drop_na_and_fill(stock_data, threshold=0.2, fill_value=0):
    # Drop stocks with more than 20% NAs
    stock_data_filtered = stock_data.dropna(thresh=int((1 - threshold) * len(stock_data)), axis=1)

    # Fill remaining NAs with 0
    stock_data_filled = stock_data_filtered.fillna(fill_value)

    return stock_data_filled

def main():
    # Read tickers from CSV file
    csv_file_path = 'nyse-listed.csv'  # Replace with your actual file path
    tickers = read_tickers_from_csv(csv_file_path)

    # Specify date range since January 1, 2000
    start_date = datetime(2000, 1, 1)
    end_date = datetime.now()

    # Download stock data
    stock_data = download_stock_data(tickers, start_date, end_date)

    # Drop stocks with more than 20% NAs and fill remaining NAs with 0s
    processed_stock_data = drop_na_and_fill(stock_data)

    # Store the processed data in a CSV file
    processed_stock_data.to_csv('processed_stock_data.csv')

    print(processed_stock_data.head())

if __name__ == "__main__":
    main()
