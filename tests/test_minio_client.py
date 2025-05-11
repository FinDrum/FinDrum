import pytest
from io import BytesIO
from unittest.mock import MagicMock, create_autospec

from minio import Minio
from findrum.clients.minio_client import MinioDataClient


@pytest.fixture
def mock_minio_client():
    return create_autospec(Minio)


@pytest.fixture
def client(mock_minio_client):
    return MinioDataClient(mock_minio_client, "test-bucket")


def test_get_object_success(client, mock_minio_client):
    mock_minio_client.bucket_exists.return_value = True
    mock_stream = BytesIO(b"minio content")
    mock_minio_client.get_object.return_value = mock_stream

    result = client.get_object("path/to/object")
    assert result.read() == b"minio content"
    mock_minio_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio_client.get_object.assert_called_once_with("test-bucket", "path/to/object")


def test_get_object_bucket_missing(client, mock_minio_client):
    mock_minio_client.bucket_exists.return_value = False

    with pytest.raises(ValueError, match="Bucket 'test-bucket' does not exist"):
        client.get_object("missing/path")


def test_put_object_success(client, mock_minio_client):
    mock_minio_client.bucket_exists.return_value = True
    data = BytesIO(b"upload content")

    client.put_object("upload/path", data, content_type="text/plain")

    mock_minio_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio_client.put_object.assert_called_once()
    args, kwargs = mock_minio_client.put_object.call_args
    assert kwargs["bucket_name"] == "test-bucket"
    assert kwargs["object_name"] == "upload/path"
    assert kwargs["content_type"] == "text/plain"
    assert kwargs["length"] == len(b"upload content")


def test_put_object_bucket_missing(client, mock_minio_client):
    mock_minio_client.bucket_exists.return_value = False
    data = BytesIO(b"some content")

    with pytest.raises(ValueError, match="Bucket 'test-bucket' does not exist"):
        client.put_object("upload/path", data)
