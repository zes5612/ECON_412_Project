import polars as pl
import matplotlib.pyplot as plt
import os 

COLORS = [
    "blue",
    "orange",
    "mediumseagreen",
    "red",
    "purple",
    "brown",
    "fuchsia",
    "gray",
    "olive",
    "aqua",
    "gold"
]

def process_agg_data(col):
    df = pl.read_csv(f"Data\Aggregated_Data\{col}_compounded_yearly_returns.csv")
    return df

def filter_top_bot(df, k):
    yearly_average = (
        df.group_by(["Sub-Industry"])
        .agg(
            pl.col("Compounded Return").mean().alias("Average return")
        )
    )
    
    top_5 = yearly_average.top_k(k, by="Average return")
    bot_5 = yearly_average.bottom_k(k, by="Average return")
    top_bot_5 = pl.concat([top_5, bot_5])["Sub-Industry"].to_list()

    return df.filter(pl.col("Sub-Industry").is_in(top_bot_5))

def line_graph(df, col):
    fig, ax = plt.subplots(figsize=(20, 10))

    for sector, df_part in df.group_by(col):
        ax.plot(
            df_part["Year"],
            df_part["Compounded Return"],
            label=sector
        )

    ax.grid(True, axis="y")
    ax.legend(title=col, loc='upper left')

    ax.set(
        xlabel="Year",
        ylabel="Compounded Yearly Returns",
        title=f"Compounded Yearly Returns of the\nS&P 500 by {col}, 10 Years"
    )

    fig.savefig(f'{col}_compounded_yearly_line.pdf')

def yearly_bar_chart(df, col):
    
    print(df["Year"])
    unique_years = df["Year"].unique().to_list()

    for year in unique_years:
        df_year = df.filter(pl.col("Year") == year)
        wrapped_labels = [ label.replace(' ', '\n') for label in df_year[col] ] #Helps formatting

        compound = df_year["Compounded Return"]
        
        fig, ax = plt.subplots(figsize=(13, 8))

        ax.bar(
            wrapped_labels,
            compound.sort(),
            color=COLORS
        )

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_position(('data', 0))

        ax.set(
            ylabel="Compounded Yearly Return",
            title=f"Compunded Return of the\nS&P 500 by Sector, {year}"
        )

        plt.tight_layout()

        fig.savefig(f'{col}_{year}_compounded_return.pdf')

def average_bar_chart(df, col):
    average_returns = (
        df.group_by([col])
        .agg(
            pl.col("Compounded Return").mean().alias("Average return")
        )
    )
    wrapped_labels = [ label.replace(' ', '\n') for label in average_returns[col] ] #Helps formatting

    fig, ax = plt.subplots(figsize=(13, 8))

    ax.bar(
        wrapped_labels,
        average_returns["Average return"].sort(),
        color=COLORS
    )

    avg = average_returns["Average return"].mean()

    # draw average line
    ax.axhline(avg, color='red', linestyle='--', linewidth=2, label=f'Average: {avg:.2f}')
    ax.legend()

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_position(('data', 0))

    ax.set(xlabel=col,
            ylabel="Average Compounded Yearly Returns",
            title=f"Average Compunded Yearly Returns of the\nS&P 500 by {col}, 10 Years")

    plt.tight_layout()
    fig.savefig(f'{col}_compounded_yearly_bar.pdf')


def create_graphs(col):
    df = process_agg_data(col)

    if(col == "Sub-Industry"):
        df = filter_top_bot(df, 5)
    
    line_graph(df, col)
    #yearly_bar_chart(df, col)
    average_bar_chart(df, col)

if __name__ == "__main__":
    create_graphs("Sub-Industry")

    
   