"""
Trade classification module
"""

import polars as pl
from datetime import timedelta
from tradedecomp.utils import str_to_timedelta

def label_trades(df: pl.DataFrame, products: list, ts_col: str, delta: str | timedelta, mapping: dict = {0: "iso", 1: "nis-c", 2: "nis-s", 3: "nis-b"}):
    """
    Label trades based on co-trading relationships

    Args:
        df (pl.DataFrame): The dataframe containing the trades
        prods (list): The list of products to label
        delta (str): The time delta to use for the rolling window
        mapping (dict): The mapping of condition codes to trade types
    Returns:
        pl.DataFrame: The dataframe with the labeled trades
    """
    if isinstance(delta, str):
        delta = str_to_timedelta(delta)
    df = df.with_columns([(pl.col("product") == prod).cast(pl.Int64).alias(f"{prod}_flag") for prod in products])  
    
    df = df.with_columns(
        [pl.sum(f"{prod}_flag").rolling(
            index_column=ts_col,
            period=2*delta,
            offset=-delta,
            closed="both"
            ).alias(f"{prod}_count") for prod in products]
    )

    flag_columns = [pl.col(f"{prod}_flag")  for prod in products]
    count_columns = [pl.col(f"{prod}_count") for prod in products]
    adjusted_counts = [(count - flag).alias(f"{prod}_count") for count, flag, prod in zip(count_columns, flag_columns, products)]
    product_exprs = [count * flag for count, flag in zip(count_columns, flag_columns)]

    df = df.with_columns(
        adjusted_counts
    ).with_columns(
        pl.sum_horizontal(count_columns).alias("total_count")
    ).with_columns(
        pl.sum_horizontal(product_exprs).alias("same_count")
    ).with_columns(
        (pl.col("total_count")-pl.col("same_count")).alias("other_count")
    ).drop(count_columns + flag_columns + ["total_count"])

    mapping_str = {str(i): v for i, v in mapping.items()}
    df = df.with_columns(
        (2 * (pl.col("same_count") > 0).cast(pl.Int8) +
        (pl.col("other_count") > 0).cast(pl.Int8)).cast(pl.Utf8).alias("trade_code")
    ).with_columns(
        pl.col("trade_code").replace(mapping_str).alias("trade_type")
    ).drop(["same_count", "other_count", "trade_code"])

    return df