# Trade Decomposition
Trade Co-occurrence, Trade Flow Decomposition, and Conditional Order Imbalance

This package contains tools based on the paper [https://arxiv.org/pdf/2209.10334](Trade Co-occurrence, Trade Flow Decomposition, and Conditional Order Imbalance in Equity Markets) by Yutong Lu, Gesine Reinert and Mihai Cucuringu. The package is managed using [uv](https://docs.astral.sh/uv/). To install, use 

`uv build'

At the moment this package works with the Tardis data provided by Terank, but in a future version it will scrape data from the repositories of ByBit and the like. 

![Sample decomposition](doc/images/trades_2025-02-23 09:00:00_0:00:30.png)

An example of how to use this package, how to load book and trade data and carry out the trade classification is found in this [notebook](examples/intro.ipynb).