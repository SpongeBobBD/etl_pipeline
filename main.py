from etl import (
    Pipeline,
    EquityExtractor,
    CryptoExtractor,
    FinancialCleaner,
    EquityTransformer,
    CryptoTransformer,
    CSVLoader,
)

START = "2024-01-01"
END = "2024-12-31"


def run_equity():
    pipeline = Pipeline(
        extractor=EquityExtractor(),
        cleaner=FinancialCleaner(),
        transformer=EquityTransformer(),
        loader=CSVLoader("output"),
    )
    df = pipeline.run("AAPL", START, END)
    print(df.tail())


def run_crypto():
    pipeline = Pipeline(
        extractor=CryptoExtractor(),
        cleaner=FinancialCleaner(),
        transformer=CryptoTransformer(),
        loader=CSVLoader("output"),
    )
    df = pipeline.run("BTC", START, END)
    print(df.tail())


if __name__ == "__main__":
    print("=== Equity: AAPL ===")
    run_equity()

    print("\n=== Crypto: BTC ===")
    run_crypto()