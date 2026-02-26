import pandas as pd
from .base import BaseTransformer


class EquityTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["daily_return"] = df["Close"].pct_change()
        df["ma_7"] = df["Close"].rolling(7).mean()
        df["ma_20"] = df["Close"].rolling(20).mean()
        df["volatility_20"] = df["daily_return"].rolling(20).std()
        return df


class CryptoTransformer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["daily_return"] = df["Close"].pct_change()
        df["ma_7"] = df["Close"].rolling(7).mean()
        df["ma_30"] = df["Close"].rolling(30).mean()
        df["volatility_7"] = df["daily_return"].rolling(7).std()

        # realized vol annualized
        df["ann_vol"] = df["volatility_7"] * (365 ** 0.5)
        return df