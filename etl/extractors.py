import pandas as pd
import requests
import yfinance as yf
from .base import BaseExtractor


class EquityExtractor(BaseExtractor):
    def extract(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start, end=end)
        if df.empty:
            raise ValueError(f"No data returned for {symbol}")
        df.index = df.index.tz_localize(None)
        df["symbol"] = symbol
        return df[["Open", "High", "Low", "Close", "Volume", "symbol"]]


class CryptoExtractor(BaseExtractor):
    BASE_URL = "https://api.coingecko.com/api/v3"

    SYMBOL_MAP = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
    }

    def extract(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        coin_id = self.SYMBOL_MAP.get(symbol.upper())
        if not coin_id:
            raise ValueError(f"Unknown crypto symbol: {symbol}. Add it to SYMBOL_MAP.")

        start_ts = int(pd.Timestamp(start).timestamp())
        end_ts = int(pd.Timestamp(end).timestamp())

        url = f"{self.BASE_URL}/coins/{coin_id}/market_chart/range"
        resp = requests.get(url, params={"vs_currency": "usd", "from": start_ts, "to": end_ts}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        prices = pd.DataFrame(data["prices"], columns=["timestamp", "Close"])
        volumes = pd.DataFrame(data["total_volumes"], columns=["timestamp", "Volume"])

        df = prices.merge(volumes, on="timestamp")
        df["Date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.normalize()
        df = df.groupby("Date").last().reset_index()
        df["symbol"] = symbol
        df = df.set_index("Date")
        return df[["Close", "Volume", "symbol"]]