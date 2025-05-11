import io
import logging
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from findrum.writers.parquet_writer import ParquetWriter


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def writer(mock_client):
    return ParquetWriter(mock_client)


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "x": [2, 1, 3],
        "y": ["b", "a", "c"]
    })


def test_write_successful(writer, mock_client, sample_df):
    writer.write("test.parquet", sample_df)

    mock_client.put_object.assert_called_once()
    args, kwargs = mock_client.put_object.call_args

    assert kwargs["path"] == "test.parquet"
    assert kwargs["content_type"] == "application/octet-stream"
    assert isinstance(kwargs["data"], io.BytesIO)
    kwargs["data"].seek(0)
    df_read_back = pd.read_parquet(kwargs["data"])
    pd.testing.assert_frame_equal(
        df_read_back.sort_values("x").reset_index(drop=True),
        sample_df.sort_values("x").reset_index(drop=True)
    )


def test_write_skips_empty_dataframe(writer, mock_client):
    empty_df = pd.DataFrame()

    with patch("logging.warning") as mock_warn:
        writer.write("empty.parquet", empty_df)
        mock_warn.assert_called_once_with("ParquetWriter skipped empty DataFrame (%s)", "empty.parquet")

    mock_client.put_object.assert_not_called()


def test_write_with_sort(writer, mock_client, sample_df):
    writer.write("sorted.parquet", sample_df, sort_by=["x"])

    args, kwargs = mock_client.put_object.call_args
    data = kwargs["data"]
    data.seek(0)
    df_sorted = pd.read_parquet(data)

    expected = sample_df.sort_values("x").reset_index(drop=True)
    pd.testing.assert_frame_equal(df_sorted.reset_index(drop=True), expected)


def test_write_with_missing_sort_column(writer, mock_client, sample_df):
    with patch("logging.warning") as mock_warn:
        writer.write("missing_col.parquet", sample_df, sort_by=["nonexistent"])

        mock_warn.assert_called_once_with(
            "Sort skipped — columns not found in DataFrame: %s", ["nonexistent"]
        )

    mock_client.put_object.assert_called_once()