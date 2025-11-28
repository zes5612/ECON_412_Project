import polars as pl
from pathlib import Path

DATA_PATH = "Data/"
TICKER_DATA_PATH = "Data/Ticker_Data/"
TICKER_DATA_FILES= list(Path(TICKER_DATA_PATH).glob("*.csv"))


def join_and_save():
    ticker_dfs = load_tickers()
    info = company_info()
    
    # master_path = os.path.join(DATA_PATH, "Master_Data")
    # os.makedirs(master_path, exist_ok=True)

    master_path = Path(DATA_PATH) / "Master_Data"
    master_path.mkdir(parents=True, exist_ok=True)
    (master_path / ".gitkeep").touch(exist_ok=True)
    

    for df in ticker_dfs:
        ticker = df['Ticker'].first()
        df = df.join(info, on="Ticker", how="left")
        
        filename = Path(master_path) / f"{ticker}.csv"
        
        df.write_csv(filename)
        print(f"Saved {ticker} to {filename}")
    
    
def load_tickers():
    dfs = []

    for path in TICKER_DATA_FILES:
        ticker = path.stem

        df = pl.read_csv(
            path,
            skip_rows=2,
            has_header=True,
            new_columns=["Date", "Close", "High", "Low", "Open", "Volume"]
            )
        
        if df.height == 0:
            print(f"{ticker} CSV is empty, skipping")
            continue
        
        df = df.with_columns(pl.lit(ticker).alias("Ticker"))
        
        try:
            validate_df(df)
        except Exception as e:
            print(f"{ticker} is not valid, skipping: {e}")
            continue
        
        dfs.append(df)
        print(f"Loaded {ticker} ({len(dfs) + 1}/{len(TICKER_DATA_PATH)})")

    return dfs


def validate_df(df):
    assert set(["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]).issubset(df.columns), "Missing required columns"
    assert df["Ticker"].n_unique() >= 1, "Ticker column has no values"
    assert df["Date"].is_sorted(), "Date column is not sorted"

    total_nulls = df.null_count().to_numpy().sum()
    assert total_nulls == 0, f"Found {total_nulls} null values"


def company_info():
    info_path = Path(DATA_PATH) / "company_list.csv"
    info = pl.read_csv(info_path)
    
    info = info.select(
        pl.col("Symbol").alias("Ticker"),
        pl.col("GICS Sector").alias("Sector"),
        pl.col("GICS Sub-Industry").alias("Sub-Industry")
        )
    
    return info
