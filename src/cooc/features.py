from datetime import timedelta
from cooc.utils import str_to_timedelta, timedelta_to_str
import polars as pl
from typing import List

# Freatures for trades

def add_size(df: pl.DataFrame, col: str='qty') -> pl.DataFrame:
    """
    Add a size column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(col).abs().alias('size'))
    return df

def add_side(df: pl.DataFrame, col: str='qty') -> pl.DataFrame:
    """
    Add a side column to the DataFrame.
    """
    df = df.with_columns(
        pl.when(pl.col(col) > 0).then(1).otherwise(-1).alias('side'))
    return df

def coi(df: pl.DataFrame, ts_col: str, delta: str | timedelta, type: str) -> pl.DataFrame:
    """
    Calculate the Conditional Order Imbalance (COI) for a given dataframe.
    """
    if isinstance(delta, str):
        delta = str_to_timedelta(delta)
    delta_str = timedelta_to_str(delta)

    assert ts_col in df.columns
    assert "qty" in df.columns

    assert type in ["nis", "nis-c", "nis-b", "nis-s"]
    
    has_side, has_size = True, True
    if "side" not in df.columns:
        has_side = False    
        df = add_side(df)
    if "size" not in df.columns:
        has_size = False
        df = add_size(df)

    df = df.with_columns([
        pl.when((pl.col("side") == 1) & (pl.col("trade_type") == type))
          .then(pl.col("size"))
          .otherwise(pl.lit(0))
          .alias("buy_filter"),
        pl.when((pl.col("side") == -1) & (pl.col("trade_type") == type))
          .then(pl.col("size"))
          .otherwise(pl.lit(0))
          .alias("sell_filter")
    ])

    buy_col = f"N_buy_{type}_{delta_str}"
    sell_col = f"N_sell_{type}_{delta_str}"

    df = df.with_columns([  
        pl.sum("buy_filter").rolling(
            index_column=ts_col,
            period=delta,
            closed="left" # don't include the current row in the sum / don't look back
            ).alias(buy_col),
        pl.sum("sell_filter").rolling(
            index_column=ts_col,
            period=delta,
            closed="left" # don't include the current row in the sum / don't look back
            ).alias(sell_col)
    ]).drop(["buy_filter", "sell_filter"])
    
    df = df.with_columns(
        pl.when(pl.col(buy_col) + pl.col(sell_col) > 0)
          .then((pl.col(buy_col) - pl.col(sell_col)) / (pl.col(buy_col) + pl.col(sell_col))
         ).otherwise(pl.lit(0)).alias(f"coi_{type}_{delta_str}")
    ).drop([buy_col, sell_col])

    if not has_side:
        df = df.drop(["side"])
    if not has_size:
        df = df.drop(["size"])

    return df

# Features for prices

def add_spread(df: pl.DataFrame, col: str='spread') -> pl.DataFrame:
    """
    Add a spread column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(col).abs().alias('spread'))
    return df

def add_volume(df: pl.DataFrame, col: str='volume') -> pl.DataFrame:
    """
    Add a volume column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(col).abs().alias('volume'))
    return df

def add_vwap(df: pl.DataFrame, col: str='vwap') -> pl.DataFrame:
    """
    Add a VWAP column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(col).alias('vwap'))
    return df

