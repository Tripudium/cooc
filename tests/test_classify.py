import pytest
import polars as pl
from cooc.classify import classify_trades

@pytest.fixture
def df():
    data = {
        "ts": [
            "2025-02-23 09:00:00",
            "2025-02-23 09:00:00",
            "2025-02-23 09:00:10",
            "2025-02-23 09:00:20",
            "2025-02-23 09:00:40",
            "2025-02-23 09:01:40",
            "2025-02-23 09:02:30",
            "2025-02-23 09:02:40",
            "2025-02-23 09:03:30",
            "2025-02-23 09:03:40"
        ],
        "product": [
            "BTCUSDT", "SOLUSDT", "SOLUSDT", "ETHUSDT", "BTCUSDT",
            "ETHUSDT", "ETHUSDT", "BTCUSDT", "SOLUSDT", "SOLUSDT"
        ],
        "trade_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "prc": [100, 102, 101, 103, 104, 103, 101, 100, 99, 98],
        "qty": [10, 15, 12, 20, -18, 21, -15, 12, -10, 10]
    }

    df = pl.DataFrame(data)
    df = df.with_columns(
        pl.col("ts").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")
    )
    return df

@pytest.fixture
def expected():
    return ['nis-c', 'nis-b', 'nis-b', 'nis-c', 'nis-c', 'iso', 'nis-c', 'nis-c', 'nis-s', 'nis-s']

def test_classify_trades(df, expected):
    prods = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    delta = "30s"

    df = classify_trades(df, prods, "ts", delta)
    assert df.shape[0] == 10
    assert df.shape[1] == 6
    assert df.columns == ['ts', 'product', 'trade_id', 'prc', 'qty', 'trade_type']
    assert df['trade_type'].to_list() == expected