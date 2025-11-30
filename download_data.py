
import yfinance as yf
import polars as pl
import os

DATA_PATH = "Data/"

def downloadData():
    tickers = getAllTickers()

    for ticker in tickers:
        ticker = ticker.replace(".", "-")
        
        df = yf.download(ticker, start="2015-01-01", end="2025-01-01", interval="1d", auto_adjust=True)
        
        file_path = os.path.join(DATA_PATH, f"Ticker_Data/{ticker}.csv")
        df.to_csv(file_path)

def getAllTickers():

    file_path = os.path.join(DATA_PATH, "company_list.csv")
    df = pl.read_csv(file_path)

    company_list = df["Symbol"].to_list()
    return company_list

if __name__ == "__main__":
    downloadData()