import polars as pl
from datetime import timedelta
import pandas as pd
import numpy as np

def label_trades(df: pl.DataFrame, products: list, ts_col: str, delta: timedelta) -> pl.DataFrame:
    """
    Label trades based on co-trading relationships
    """
    df = (
        df.with_columns(
            [
                (pl.col("product") == prod)
                .cast(pl.Int64)
                .alias(f"{prod}_flag")
                for prod in products
            ]
        )
        .with_columns(
            [
                pl.sum(f"{prod}_flag")
                .rolling(
                    index_column=ts_col,
                    period=2 * delta,
                    offset=-delta,
                    closed="both",
                )
                .alias(f"{prod}_count")
                for prod in products
            ]
        )
    )

    flag_columns = [pl.col(f"{prod}_flag") for prod in products]
    count_columns = [pl.col(f"{prod}_count") for prod in products]
    adjusted_counts = [(count - flag).alias(f"{prod}_count") 
                       for count, flag, prod in zip(count_columns, flag_columns, products)]
    product_exprs = [count * flag for count, flag in zip(count_columns, flag_columns)]

    df = (
        df.with_columns(adjusted_counts)
        .with_columns(pl.sum_horizontal(count_columns).alias("total_count"))
        .with_columns(pl.sum_horizontal(product_exprs).alias("same_count"))
        .with_columns((pl.col("total_count") - pl.col("same_count")).alias("other_count"))
        .drop(count_columns + flag_columns + ["total_count"])
    )

    TRADE_TYPE_MAPPING: dict[str, str] = {"0": "iso", "1": "nis-c", "2": "nis-s", "3": "nis-b"}

    df = (
        df.with_columns(
            (
                2 * (pl.col("same_count") > 0).cast(pl.Int8) +
                (pl.col("other_count") > 0).cast(pl.Int8)
            ).cast(pl.Utf8).alias("trade_code")
        )
        .with_columns(pl.col("trade_code").replace(TRADE_TYPE_MAPPING).alias("trade_type"))
        .drop(["same_count", "other_count", "trade_code"])
    )

    return df

def label_trades_for_delta(df_trades: pd.DataFrame, delta_ns: float) -> pd.DataFrame:
    """
    Labels trades as 'iso', 'nis-s', 'nis-c', or 'nis-b' for a given delta 
    (time window +/- delta_ns around each trade).
    """
    # Ensure the 'dts' column is properly converted to datetime
    df_trades["dts"] = pd.to_datetime(df_trades["dts"], errors="coerce")
    
    # Convert timestamps to integer nanoseconds
    df_trades["dts_ns"] = df_trades["dts"].view(np.int64)

    # Sort by timestamp
    df_trades = df_trades.sort_values("dts_ns").reset_index(drop=True)

    # Extract time column as NumPy array (in nanoseconds)
    times = df_trades["dts_ns"].values

    # Initialize the co-occurrence type array
    cooc_types = np.empty(len(df_trades), dtype=object)

    # Iterate over trades and classify
    for i in range(len(df_trades)):
        t = times[i]
        prod = df_trades.at[i, "product"]

        left_bound = t - delta_ns
        right_bound = t + delta_ns

        # Perform binary search for the time window
        start_idx = np.searchsorted(times, left_bound, side="left")
        end_idx = np.searchsorted(times, right_bound, side="right")

        # Get neighboring trades, excluding the current row
        if start_idx <= i < end_idx:
            neighbors = df_trades.iloc[
                list(range(start_idx, i)) + list(range(i + 1, end_idx))
            ]
        else:
            neighbors = df_trades.iloc[start_idx:end_idx]

        unique_neighbor_prods = neighbors["product"].unique()

        # Determine co-occurrence type
        if len(unique_neighbor_prods) == 0:
            cooc_type = "iso"
        elif len(unique_neighbor_prods) == 1:
            cooc_type = "nis-s" if prod in unique_neighbor_prods else "nis-c"
        else:
            cooc_type = "nis-b"

        cooc_types[i] = cooc_type

    df_trades["cooc_type"] = cooc_types
    return df_trades