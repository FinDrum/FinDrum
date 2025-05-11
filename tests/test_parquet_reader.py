import pandas as pd
import pytest
from io import BytesIO
from unittest.mock import MagicMock

from findrum.readers.parquet_reader import ParquetReader


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def sample_parquet_bytes():
    """Genera un archivo Parquet en memoria con pandas + pyarrow."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "age": [30, 40]
    })
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow")
    buffer.seek(0)
    return buffer


def test_read_full_parquet(mock_client, sample_parquet_bytes):
    mock_client.get_object.return_value = sample_parquet_bytes
    reader = ParquetReader(mock_client)

    df = reader.read("dummy/path.parquet")

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"name", "age"}
    assert df.shape == (2, 2)
    mock_client.get_object.assert_called_once_with("dummy/path.parquet")


def test_read_selected_columns(mock_client, sample_parquet_bytes):
    mock_client.get_object.return_value = sample_parquet_bytes
    reader = ParquetReader(mock_client)

    df = reader.read("dummy/path.parquet", columns=["name"])

    assert list(df.columns) == ["name"]
    assert df.shape == (2, 1)


def test_read_file_not_found(mock_client):
    mock_client.get_object.side_effect = FileNotFoundError("not found")
    reader = ParquetReader(mock_client)

    with pytest.raises(FileNotFoundError):
        reader.read("missing/path.parquet")


def test_read_invalid_parquet(mock_client):
    mock_client.get_object.return_value = BytesIO(b"not parquet data")
    reader = ParquetReader(mock_client)

    with pytest.raises(Exception):
        reader.read("invalid/path.parquet")
