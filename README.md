# ETL Pipeline for Financial Time Series

A modular ETL framework for pulling, cleaning, transforming, and storing financial time series data. Each module is independently swappable — swap in a different data source, storage backend, or transformation logic without touching the rest.

## Structure

```
etl/
├── base.py          # Abstract interfaces
├── extractors.py    # Data sources (yfinance, CoinGecko)
├── cleaners.py      # Data cleaning
├── transformers.py  # Feature engineering
├── loaders.py       # Storage backends (CSV, SQLite)
└── pipeline.py      # Orchestrator
tests/
└── test_pipeline.py
main.py
```

## Quickstart

```bash
pip install -r requirements.txt
python main.py
```

This runs two pipelines — AAPL (equity) and BTC (crypto) — and saves results to `output/`.

## Running Tests

```bash
pytest tests/ -v
```

Tests use mocked/synthetic data so no network calls are needed.

## Supported Assets

**Equities** — any ticker supported by Yahoo Finance (e.g. `AAPL`, `TSLA`, `SPY`)

**Crypto** — BTC, ETH, SOL via CoinGecko free API. Add more in `CryptoExtractor.SYMBOL_MAP`.

## Extending

Swap any module by subclassing the relevant base class:

```python
from etl.base import BaseLoader

class PostgresLoader(BaseLoader):
    def load(self, df, name):
        # your implementation
        ...
```

Then pass it into `Pipeline(...)` as usual.

## Output

Each run produces a CSV (or SQLite table) with OHLCV data plus computed features:

| Column | Description |
|---|---|
| `daily_return` | Day-over-day % change |
| `ma_7` / `ma_20` | Rolling moving averages |
| `volatility_20` | Rolling std of returns |
| `ann_vol` | Annualized volatility (crypto only) |