from typing import Optional
from .base import BaseExtractor, BaseCleaner, BaseTransformer, BaseLoader
import pandas as pd


class Pipeline:
    def __init__(
        self,
        extractor: BaseExtractor,
        cleaner: BaseCleaner,
        transformer: BaseTransformer,
        loader: BaseLoader,
    ):
        self.extractor = extractor
        self.cleaner = cleaner
        self.transformer = transformer
        self.loader = loader

    def run(self, symbol: str, start: str, end: str, table_name: Optional[str] = None) -> pd.DataFrame:
        name = table_name or symbol.lower()
        print(f"[{name}] extracting...")
        raw = self.extractor.extract(symbol, start, end)

        print(f"[{name}] cleaning...")
        cleaned = self.cleaner.clean(raw)

        print(f"[{name}] transforming...")
        transformed = self.transformer.transform(cleaned)

        print(f"[{name}] loading...")
        self.loader.load(transformed, name)

        return transformed