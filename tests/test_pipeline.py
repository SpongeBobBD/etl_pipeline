import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from etl.cleaners import FinancialCleaner
from etl.transformers import EquityTransformer, CryptoTransformer
from etl.loaders import CSVLoader, SQLiteLoader
from etl.pipeline import Pipeline
from etl.base import BaseExtractor, BaseCleaner, BaseTransformer, BaseLoader


def make_price_df(n=30, symbol="TEST"):
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    df = pd.DataFrame({
        "Open": np.random.uniform(100, 200, n),
        "High": np.random.uniform(200, 250, n),
        "Low": np.random.uniform(80, 100, n),
        "Close": np.random.uniform(100, 200, n),
        "Volume": np.random.randint(1_000_000, 10_000_000, n),
        "symbol": symbol,
    }, index=dates)
    return df


class TestFinancialCleaner:
    def test_removes_duplicates(self):
        df = make_price_df(10)
        df = pd.concat([df, df.iloc[:2]])
        cleaned = FinancialCleaner().clean(df)
        assert not cleaned.index.duplicated().any()

    def test_fills_missing_values(self):
        df = make_price_df(10)
        df.loc[df.index[3], "Close"] = np.nan
        cleaned = FinancialCleaner().clean(df)
        assert cleaned["Close"].isna().sum() == 0

    def test_removes_zero_prices(self):
        df = make_price_df(10)
        df.loc[df.index[2], "Close"] = 0
        cleaned = FinancialCleaner().clean(df)
        assert (cleaned["Close"] > 0).all()

    def test_sorts_by_date(self):
        df = make_price_df(10)
        df = df.iloc[::-1]
        cleaned = FinancialCleaner().clean(df)
        assert cleaned.index.is_monotonic_increasing


class TestEquityTransformer:
    def test_adds_expected_columns(self):
        df = make_price_df(30)
        result = EquityTransformer().transform(df)
        for col in ["daily_return", "ma_7", "ma_20", "volatility_20"]:
            assert col in result.columns

    def test_daily_return_range(self):
        df = make_price_df(30)
        result = EquityTransformer().transform(df)
        # returns should be reasonable
        assert result["daily_return"].dropna().abs().max() < 1.0


class TestCryptoTransformer:
    def test_adds_expected_columns(self):
        df = make_price_df(35)
        result = CryptoTransformer().transform(df)
        for col in ["daily_return", "ma_7", "ma_30", "ann_vol"]:
            assert col in result.columns


class TestCSVLoader:
    def test_creates_file(self, tmp_path):
        loader = CSVLoader(str(tmp_path))
        df = make_price_df(5)
        loader.load(df, "test_asset")
        assert (tmp_path / "test_asset.csv").exists()


class TestSQLiteLoader:
    def test_creates_table(self, tmp_path):
        import sqlite3
        db_path = str(tmp_path / "test.db")
        loader = SQLiteLoader(db_path)
        df = make_price_df(5)
        loader.load(df, "equity_test")
        with sqlite3.connect(db_path) as conn:
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        assert ("equity_test",) in tables


class TestPipeline:
    def test_run_returns_dataframe(self, tmp_path):
        raw_df = make_price_df(30)

        class DummyExtractor(BaseExtractor):
            def extract(self, symbol, start, end):
                return raw_df

        class DummyLoader(BaseLoader):
            def load(self, df, name):
                pass

        pipeline = Pipeline(
            extractor=DummyExtractor(),
            cleaner=FinancialCleaner(),
            transformer=EquityTransformer(),
            loader=DummyLoader(),
        )
        result = pipeline.run("DUMMY", "2024-01-01", "2024-12-31")
        assert isinstance(result, pd.DataFrame)
        assert "daily_return" in result.columns