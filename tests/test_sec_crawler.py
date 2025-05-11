import io
import zipfile
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from findrum.crawlers.sec_crawler import SecCrawler


@pytest.fixture
def valid_zip_bytes():
    """Returns a ZIP file in memory with one valid JSON and one invalid."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr("CIK123456789.json", b'{"entityName": "Test Corp", "facts": {"Revenue": {"2023": 1000}}}')
        zip_file.writestr("README.txt", b"This is not JSON")
    buffer.seek(0)
    return buffer.getvalue()


@pytest.fixture
def mocked_response(valid_zip_bytes):
    """Mocked HTTP response returning the in-memory ZIP."""
    mock_resp = MagicMock()
    mock_resp.content = valid_zip_bytes
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_fetch_yields_dataframes(mocked_response):
    with patch("requests.Session.get", return_value=mocked_response):
        crawler = SecCrawler(email="test@example.com")
        dfs = list(crawler.fetch())

        assert len(dfs) == 1
        df = dfs[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["cik", "entity_name", "facts"]
        assert df.iloc[0]["cik"] == "0123456789"
        assert df.iloc[0]["entity_name"] == "Test Corp"
        assert isinstance(df.iloc[0]["facts"], dict)


def test_fetch_skips_non_json(mocked_response):
    with patch("requests.Session.get", return_value=mocked_response):
        crawler = SecCrawler(email="test@example.com")
        dfs = list(crawler.fetch())

        assert len(dfs) == 1
        assert dfs[0].iloc[0]["entity_name"] == "Test Corp"


def test_fetch_raises_on_http_error():
    with patch("requests.Session.get") as mock_get:
        mock_get.side_effect = Exception("Connection failed")

        crawler = SecCrawler(email="test@example.com")
        with pytest.raises(Exception, match="Connection failed"):
            list(crawler.fetch())


def test_extract_cik_valid():
    cik = SecCrawler._extract_cik("CIK123456789.json")
    assert cik == "0123456789"


def test_extract_cik_missing():
    cik = SecCrawler._extract_cik("invalid_name.txt")
    assert cik == "0000000000"


def test_init_without_email_raises():
    with pytest.raises(ValueError):
        SecCrawler(email="")

def test_fetch_logs_json_decode_error():
    corrupted_json_zip = io.BytesIO()
    with zipfile.ZipFile(corrupted_json_zip, "w") as zf:
        zf.writestr("CIK123456789.json", b"{not valid json}")
    corrupted_json_zip.seek(0)

    response = MagicMock()
    response.content = corrupted_json_zip.getvalue()
    response.raise_for_status = MagicMock()

    with patch("requests.Session.get", return_value=response), \
         patch("logging.warning") as mock_warn:
        crawler = SecCrawler(email="test@example.com")
        results = list(crawler.fetch())

        assert results == []
        assert mock_warn.call_count == 1
        assert "Could not process" in mock_warn.call_args[0][0]


def test_fetch_logs_generic_exception():
    broken_zip = io.BytesIO()
    with zipfile.ZipFile(broken_zip, "w") as zf:
        zf.writestr("CIK123456789.json", b"{}")
    broken_zip.seek(0)

    response = MagicMock()
    response.content = broken_zip.getvalue()
    response.raise_for_status = MagicMock()

    with patch("requests.Session.get", return_value=response), \
         patch("zipfile.ZipFile.open", side_effect=RuntimeError("Simulated failure")), \
         patch("logging.exception") as mock_log:
        crawler = SecCrawler(email="test@example.com")
        results = list(crawler.fetch())

        assert results == []
        mock_log.assert_called_once()
        assert "Unexpected error while processing" in mock_log.call_args[0][0]
