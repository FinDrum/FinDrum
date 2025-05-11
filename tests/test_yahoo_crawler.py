import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from findrum.crawlers.yahoo_crawler import YahooCrawler
from yfinance.exceptions import YFRateLimitError


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "Open": [100],
        "Close": [110],
        "Volume": [1000]
    })


def test_fetch_returns_dataframe(sample_data):
    with patch("yfinance.Ticker") as mock_ticker, \
         patch("curl_cffi.requests.Session") as mock_session:

        mock_ticker.return_value.history.return_value = sample_data
        crawler = YahooCrawler(period="1mo")
        results = list(crawler.fetch(["AAPL"]))

        assert len(results) == 1
        df = results[0]
        assert isinstance(df, pd.DataFrame)
        assert "symbol" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"


def test_fetch_warns_on_empty_data():
    with patch("yfinance.Ticker") as mock_ticker, \
         patch("curl_cffi.requests.Session") as mock_session, \
         patch("logging.warning") as mock_warning:

        mock_ticker.return_value.history.return_value = pd.DataFrame()
        crawler = YahooCrawler()
        results = list(crawler.fetch(["AAPL"]))

        assert results == []
        mock_warning.assert_called_once()


def test_fetch_retries_on_rate_limit(sample_data):
    with patch("yfinance.Ticker") as mock_ticker, \
         patch("curl_cffi.requests.Session") as mock_session, \
         patch("time.sleep") as mock_sleep:

        history = mock_ticker.return_value.history
        history.side_effect = [YFRateLimitError(), sample_data]

        crawler = YahooCrawler()
        results = list(crawler.fetch(["AAPL"]))

        assert len(results) == 1
        assert "symbol" in results[0].columns
        assert mock_sleep.called


def test_fetch_logs_exception():
    with patch("yfinance.Ticker") as mock_ticker, \
         patch("curl_cffi.requests.Session") as mock_session, \
         patch("logging.exception") as mock_log:

        mock_ticker.return_value.history.side_effect = RuntimeError("test error")

        crawler = YahooCrawler()
        results = list(crawler.fetch(["AAPL"]))

        assert results == []
        assert mock_log.called


def test_fetch_raises_on_empty_symbols():
    crawler = YahooCrawler()
    with pytest.raises(ValueError):
        list(crawler.fetch([]))
