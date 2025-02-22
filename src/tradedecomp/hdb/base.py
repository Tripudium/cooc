from pathlib import Path
import logging
from typing import List
import polars as pl
import datetime as dt
import re   

from .utils import get_months, nanoseconds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DATA_PATH = Path(__file__).parent.parent.parent.parent / "data"

class DataLoader:
    def __init__(self, root: str | Path = DATA_PATH):
        """
        Initialize the DataLoader with a path to the data.
        """
        logger.info("Initializing DataLoader with path %s"%root)
        self.root = root
        self.raw_path = root / "raw"
        self.processed_path = root / "processed"

    @property
    def raw_path(self):
        return self._raw_path
    
    @property
    def processed_path(self):
        return self._processed_path

    @raw_path.setter
    def raw_path(self, path: str | Path):
            self._raw_path = path

    @processed_path.setter
    def processed_path(self, path: str | Path):
        self._processed_path = path

    def _load_data(self, product: str, times: List[str], type: str) -> pl.DataFrame:    
        """
        Load data for a given product and times.
        """
        if len(times) != 2:
            raise ValueError("Times must be a list of two strings in the format '%y%m%d.%H%M'")
        try:
            dtimes = [dt.datetime.strptime(t, "%y%m%d.%H%M") for t in times]
        except ValueError:
            raise ValueError("Times must be in the format '%y%m%d.%H%M'")
        months = get_months(dtimes[0], dtimes[1])

        dfs = []
        for month in months:
            filename = "{}/{}_{}_{}.parquet".format(str(self.processed_path), product, month, type)
            if not Path(filename).exists():
                logger.info(f"File {filename} not found, downloading...")
                self.download(product, month, type)
                logger.info("File downloaded, processing...")
                df = self.process(product, month, type)
                if df is None:
                    logger.info(f"Product {product} with type {type} and month {month} is not available")
                    return None
            else:
                df = pl.read_parquet(filename)
            dfs.append(df)

        df = pl.concat(dfs)
        df = df.filter(pl.col('ts').is_between(nanoseconds(times[0]), nanoseconds(times[1])))
        return df
    
    def load_trades(self, product: str, times: List[str]) -> pl.DataFrame:
        """
        Load trades data for a given product and times.
        """
        return self._load_data(product, times, "trade")    

    def load_book(self, product: str, times: List[str]) -> pl.DataFrame:
        """
        Load book data for a given product and times.
        """
        return self._load_data(product, times, "book")
    
    def download(self, product: str, month: str, type: str):
        pass

    def process(self, product: str, month: str, type: str):
        return None