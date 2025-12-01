import polars as pl
from pathlib import Path
import glob


MASTER_DATA_FILES = list(Path("Data/Master_Data").glob("*.csv"))

#Saves compounded yearly return of each compant to a csv
def save_agg(col):
    df = master_df()
    df = add_columns(df)
    yearly_returns = compounded_yearly_returns(df, col)

    aggregates_path = Path("Data/Aggregated_Data")
    aggregates_path.mkdir(parents=True, exist_ok=True)
    (aggregates_path / ".gitkeep").touch(exist_ok=True)

    filename = Path(aggregates_path) / f"{col}_compounded_yearly_returns.csv"
    yearly_returns.write_csv(filename)


def compounded_yearly_returns(master_df, col):
    daily_returns = master_df.group_by(["Date", col]).agg(
        pl.col("Daily Return").mean().alias(col + " Daily Return")
    )
    daily_returns = daily_returns.with_columns(
        pl.col("Date").dt.year().alias("Year") 
    )
    
    #Add 1 to multiply growth factors instead of daily returns (1.05, 1.02, etc instead of 0.05, 0.02, etc)
    #After finding cumulative product, subtract 1 to return back to return instead of growth
    yearly_returns = daily_returns.group_by(["Year", col]).agg([
        ((1 + pl.col(col + " Daily Return")).cum_prod().last() - 1).alias("Compounded Return")
    ]).drop_nulls().sort(["Year", col])
    
    return yearly_returns

#Adds the daily return column and converts the year column to a date
def add_columns(master_df):
    master_df = master_df.with_columns(pl.col("Close")
                                       .pct_change().over("Ticker")
                                       .fill_null(0).alias("Daily Return"))
    
    return master_df

#Puts all files into one master dataframe with proper data format, year and daily return
def master_df():
    files = glob.glob("Data/Master_Data/*.csv")
    dfs = [pl.read_csv(file) for file in files]

    master_df = pl.concat(dfs)
    master_df = master_df.with_columns(pl.col("Date").str.strptime(pl.Date, "%Y-%m-%d"))

    return add_columns(master_df)


if __name__ == "__main__":
    save_agg("Sub-Industry")
    save_agg("Sector") 

      
