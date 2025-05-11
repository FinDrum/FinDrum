import os
import pytest
from io import BytesIO
from unittest import mock

from findrum.clients.local_client import LocalDataClient


@pytest.fixture
def client():
    return LocalDataClient()


def test_get_object_file_exists(tmp_path, client):
    file_path = tmp_path / "test.bin"
    file_path.write_bytes(b"test content")

    with mock.patch.object(client, "_ensure_directory_exists") as mock_check:
        with client.get_object(str(file_path)) as f:
            content = f.read()

    assert content == b"test content"
    mock_check.assert_called_once_with(str(file_path))


def test_get_object_file_not_found(tmp_path, client):
    fake_file = tmp_path / "missing.bin"
    os.makedirs(tmp_path, exist_ok=True)

    with mock.patch.object(client, "_ensure_directory_exists"):
        with pytest.raises(FileNotFoundError):
            client.get_object(str(fake_file))


def test_put_object_creates_file(tmp_path, client):
    file_path = tmp_path / "output.bin"
    data = BytesIO(b"some binary data")

    client.put_object(str(file_path), data)

    with open(file_path, "rb") as f:
        assert f.read() == b"some binary data"


def test_put_object_creates_directories(tmp_path, client):
    nested_path = tmp_path / "nested/dir/output.bin"
    data = BytesIO(b"nested data")

    client.put_object(str(nested_path), data)

    with open(nested_path, "rb") as f:
        assert f.read() == b"nested data"


def test_ensure_directory_exists_valid(tmp_path, client):
    file_path = tmp_path / "file.txt"
    os.makedirs(tmp_path, exist_ok=True)

    client._ensure_directory_exists(str(file_path))


def test_ensure_directory_exists_invalid(tmp_path, client):
    file_path = tmp_path / "nonexistent/file.txt"

    with pytest.raises(NotADirectoryError):
        client._ensure_directory_exists(str(file_path))
