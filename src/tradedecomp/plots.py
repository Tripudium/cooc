import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import timedelta, datetime    
from .utils import str_to_timedelta
import logging

logger = logging.getLogger(__name__)

def plot_trades(df: pl.DataFrame, products: list[str], ts_col: str, start_time: str | datetime, nr_of_trades: int, delta: str | timedelta, save=False):
    """
    Plot the decomposition of the trades into components
    """

    # Filter for the relevant trades
    if not isinstance(df[ts_col].dtype, pl.Datetime):
        logger.warning(f"Column {ts_col} is not a datetime")
        return
    if isinstance(start_time, str):
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    df = df.filter(pl.col(ts_col) >= start_time)
    df = df[:nr_of_trades]
    if isinstance(delta, str):
        delta = str_to_timedelta(delta)

    # Create a mapping from product to colour
    unique_products = df['product'].unique().to_list()
    colours = sns.color_palette("hls", n_colors=len(unique_products))
    prod_color_dict = dict(zip(unique_products, colours))
    
    # Create a mapping from product to y-axis position
    product_to_y = {product: i+1 for i, product in enumerate(products)}
    df  = df.with_columns(
        pl.col("product").replace(product_to_y).cast(pl.Float64).alias("y")
    )

    fig, ax = plt.subplots(figsize=(10, 4))
    
    ax.scatter(df['ts'], df['y'], color='blue', zorder=3)

    for y in product_to_y.values():
        ax.axhline(y=y, color='grey', linestyle='--', alpha=0.7, zorder=1)
    ax.set_yticks(list(product_to_y.values()))
    ax.set_yticklabels(list(product_to_y.keys()))
    ax.set_ylim(0, max(product_to_y.values())+1)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    fig.autofmt_xdate()

    for row in df.iter_rows(named=True):
        ax.axvspan(
            row['ts'] - delta,   
            row['ts'] + delta, 
            # Here we set the vertical extent so that the bar is drawn around the point. 
            # Since the y-axis is in data coordinates (0,1,2), we can use a small range, 
            # for example from y-0.1 to y+0.1.
            ymin=(row['y'] / (max(product_to_y.values())+1))-0.1,
            ymax=(row['y'] / (max(product_to_y.values())+1))+0.1,
            #ymin=0,
            #ymax=1,
            color=prod_color_dict[row['product']],
            alpha=0.2,
            zorder=2
        )

    # Format the y-axis with product names instead of numbers.
    ax.set_yticks(list(product_to_y.values()))
    ax.set_yticklabels(list(product_to_y.keys()))

    # Format the x-axis for better date presentation.
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    fig.autofmt_xdate()

    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Product")
    ax.set_title("Trades and their neighbourhoods")

    if save:
        plt.savefig(f"trades_{start_time}_{delta}.png")
    plt.show()