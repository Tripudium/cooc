import polars as pl
from typing import List

# Register a custom namespace for our additional DataFrame functionality.
@pl.api.register_dataframe_namespace("_dt")
class DatetimeMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def add_datetime(self, ts_col: str='ts') -> pl.DataFrame:
        """
        Add a datetime column to the DataFrame.
        """
        # Use with_column to add a new column with a literal value.
        return self._df.with_columns([pl.from_epoch(ts_col, time_unit='ns').alias('dts')])

@pl.api.register_dataframe_namespace("feature")
class FeatureMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def add_mid(self, column_names: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a mid column to the DataFrame.
        """
        return self._df.with_columns(
            ((pl.col(column_names[0]) + pl.col(column_names[1])) / 2).alias('mid'))

    def add_spread(self, column_names: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a spread column to the DataFrame.
        """
        return self._df.with_columns(
            (pl.col(column_names[0]) - pl.col(column_names[1])).alias('spread'))
    
    def add_volume(self, column_names: List[str]=['vol__s0', 'vol__s1']) -> pl.DataFrame:
        """
        Add a volume column to the DataFrame.
        """
        return self._df.with_columns(
            (pl.col(column_names[0]) + pl.col(column_names[1])).alias('volume'))

    def add_vwap(self, column_names: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a VWAP column to the DataFrame.
        """
        return self._df.with_columns(
            (pl.col(column_names[0]) + pl.col(column_names[1])).alias('vwap'))
