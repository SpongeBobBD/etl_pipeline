from abc import ABC, abstractmethod
import pandas as pd


class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        pass


class BaseCleaner(ABC):
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


class BaseLoader(ABC):
    @abstractmethod
    def load(self, df: pd.DataFrame, name: str) -> None:
        pass