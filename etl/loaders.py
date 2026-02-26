import os
import sqlite3
import pandas as pd
from .base import BaseLoader


class CSVLoader(BaseLoader):
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def load(self, df: pd.DataFrame, name: str) -> None:
        path = os.path.join(self.output_dir, f"{name}.csv")
        df.to_csv(path)
        print(f"Saved {len(df)} rows to {path}")


class SQLiteLoader(BaseLoader):
    def __init__(self, db_path: str = "output/financial_data.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path

    def load(self, df: pd.DataFrame, name: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            df.to_sql(name, conn, if_exists="replace")
        print(f"Saved {len(df)} rows to table '{name}' in {self.db_path}")