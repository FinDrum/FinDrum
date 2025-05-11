import logging
import time
from curl_cffi import requests
from typing import Generator, Iterable

import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError

from .crawler import Crawler


class YahooCrawler(Crawler):
    """
    A crawler for retrieving historical stock data from Yahoo Finance.

    Parameters
    ----------
    period : str, default "max"
        Timespan accepted by `yfinance.Ticker.history`, such as "1y", "6mo", etc.
    auto_adjust : bool, default True
        Whether to automatically adjust prices for splits and dividends.
    delay : float, default 2.0
        Seconds to wait between requests to reduce the likelihood of rate-limiting.
    """

    _DEFAULT_PERIOD = "max"

    def __init__(self, period: str = _DEFAULT_PERIOD) -> None:
        """
        Initialize the YahooCrawler instance.

        Parameters
        ----------
        period : str, optional
            Timespan accepted by `yfinance.Ticker.history`, by default "max".
        """
        self._period = period

    def fetch(self, symbols: Iterable[str]) -> Generator[pd.DataFrame, None, None]:
        """
        Retrieve historical stock data for a collection of ticker symbols.

        Parameters
        ----------
        symbols : Iterable[str]
            A collection of ticker symbols to fetch data for.

        Yields
        ------
        pd.DataFrame
            A DataFrame containing historical data for one symbol, with a column `symbol` added.

        Raises
        ------
        ValueError
            If `symbols` is empty or not iterable.
        """
        if not symbols:
            raise ValueError("'symbols' must be a non-empty iterable of ticker strings.")

        session = requests.Session(impersonate="chrome") # Fix to latest YahooFinance update

        for symbol in symbols:
            for attempt in range(3):
                try:
                    data = yf.Ticker(symbol, session=session).history(
                        period=self._period,
                    )

                    if data.empty:
                        logging.warning("Yahoo Finance returned no data for %s", symbol)
                        break

                    data = data.copy()
                    data["symbol"] = symbol
                    yield data
                    break

                except YFRateLimitError:
                    wait_time = 5 * (attempt + 1)
                    logging.warning("Rate limit hit for %s. Retrying in %s seconds...", symbol, wait_time)
                    time.sleep(wait_time)

                except Exception:
                    logging.exception("Error fetching data for %s", symbol)
                    break