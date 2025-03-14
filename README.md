# Trade Decomposition
Trade Co-occurrence, Trade Flow Decomposition, and Conditional Order Imbalance

This package contains tools based on the paper [Trade Co-occurrence, Trade Flow Decomposition, and Conditional Order Imbalance in Equity Markets](https://arxiv.org/pdf/2209.10334) by Yutong Lu, Gesine Reinert and Mihai Cucuringu. The package is managed using [uv](https://docs.astral.sh/uv/). To install, use 

```uv sync```

![Sample decomposition](https://github.com/Tripudium/tradedecomp/blob/main/doc/images/trades_2025-02-23%2009%3A00%3A00_0%3A00%3A30.png)

An example of how to use this package, how to load book and trade data and carry out the trade classification is found in this [notebook](examples/intro.ipynb).

To handle data, this package makes use of ```dspy```:

```git clone git@github.com:Tripudium/dspy.git
cd /path/to/dspy/
uv sync
uv build
uv pip install -e /path/to/dspy```

Then in the current path

```uv add /path/to/dspy```

There is probably a more elegant way of doing this using paths.