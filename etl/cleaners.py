import pandas as pd
from .base import BaseCleaner


class FinancialCleaner(BaseCleaner):
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

        numeric_cols = df.select_dtypes(include="number").columns
        df[numeric_cols] = df[numeric_cols].ffill().bfill()

        # drop rows where price is zero or negative
        if "Close" in df.columns:
            df = df[df["Close"] > 0]

        return df