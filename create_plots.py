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

    df = df.with_columns(
         (((1 + pl.col("Compounded Return")) ** 10) - 1).alias("Total Return")
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

    fig.savefig(f'{col}_compounded_yearly_line.png')
    plt.close("all")

def yearly_bar_chart(df, col):
    
    unique_years = df["Year"].unique().to_list()

    for year in unique_years:
        df_year = df.filter(pl.col("Year") == year).sort("Compounded Return")

        wrapped_labels = [ label.replace(' ', '\n') for label in df_year[col] ] #Helps formatting
        compound = df_year["Compounded Return"]
        
        fig, ax = plt.subplots(figsize=(13, 8))

        ax.bar(
            wrapped_labels,
            compound,
            color = plt.cm.tab20(range(len(wrapped_labels)))
        )

        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_position(('data', 0))

        ax.set(
            ylabel="Compounded Yearly Return",
            title=f"Compunded Return of the\nS&P 500 by {col}, {year}"
        )

        plt.tight_layout()

        fig.savefig(f'{col}_{year}_compounded_return.png')
        plt.close("all")

def average_bar_chart(df, col):
    average_returns = (
        df.group_by([col])
        .agg(
            pl.col("Compounded Return").mean().alias("Average return")
        )
    ).sort("Average return")
    wrapped_labels = [ label.replace(' ', '\n') for label in average_returns[col] ] #Helps formatting

    fig, ax = plt.subplots(figsize=(13, 8))

    ax.bar(
        wrapped_labels,
        average_returns["Average return"],
        color = plt.cm.tab20(range(len(wrapped_labels)))
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
    fig.savefig(f'{col}_compounded_yearly_bar.png')
    plt.close("all")

def compound_bar_chart(df):
    df = df.group_by("Sub-Industry").agg(
            ((1 + pl.col("Compounded Return")).product() - 1)
            .alias("Total Return_10yr")
        )
    
    top_5 = df.top_k(5, by="Total Return_10yr")
    bot_5 = df.bottom_k(5, by="Total Return_10yr")
    top_bot_5 = pl.concat([top_5, bot_5])

    fig, ax = plt.subplots(figsize=(13, 8))

    wrapped_labels = [ label.replace(' ', '\n') for label in top_bot_5["Sub-Industry"] ] #Helps formatting

    ax.bar(
        wrapped_labels,
        top_bot_5["Total Return_10yr"],
        color=plt.cm.tab20(range(len(wrapped_labels)))
    )

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_position(('data', 0))

    ax.set(xlabel="Sub-Industry",
            ylabel="Compounded Yearly Returns",
            title="Compunded Yearly Returns of the\nS&P 500 by Sub-Industry, 10 Years")

    plt.tight_layout()
    fig.savefig(f'total_return.png')
    plt.close("all")

def create_scatter(df, col, correlate):
    sectors = df[col].unique().to_list()
    colors = plt.cm.tab20(range(len(sectors)))  #Gets distinct colors for each secror
    
    fig, ax = plt.subplots(figsize=(12, 7))

    legend_labels = []

    for sector, color in zip(sectors, colors):
        df_sector = df.filter(pl.col(col) == sector)

        x = df_sector[correlate].to_numpy()
        y = df_sector["Compounded Return"].to_numpy()

        ax.scatter(x, y, color=color, alpha=0.7)

        if len(df_sector) > 1:
                corr = float(df_sector.select(
                pl.corr(correlate, "Compounded Return")
                ).item())
        else:
                corr = float("nan")

        legend_labels.append(f"{sector}  (r={corr:.2f})")

        ax.set_xlabel(correlate)
        ax.set_ylabel("Compounded Return")
        ax.set_title(f"{correlate} vs Compounded Return by {col}\nColored by {col}")

        # Build legend
        ax.legend(
                legend_labels,
                title=f"{correlate}-Growth Correlations",
                loc="upper right",
                fontsize=9
        )

        plt.tight_layout()
    fig.savefig(f'{col}_{correlate}_return_scatter.png')
    plt.close("all")

def create_graphs(col):
    df = process_agg_data(col)

    if(col == "Sub-Industry"):
        compound_bar_chart(df)
        df = filter_top_bot(df, 5)
    
    # line_graph(df, col)
    # yearly_bar_chart(df, col)
    #average_bar_chart(df, col)
    # create_scatter(df, col, "Total Volume")
    # create_scatter(df, col, "Annual Volatility")

if __name__ == "__main__":
    create_graphs("Sub-Industry")

    
   