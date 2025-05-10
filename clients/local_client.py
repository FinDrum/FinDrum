import os
from typing import BinaryIO
from io import BytesIO

from .client import DataClient


class LocalDataClient(DataClient):
    """
    Implements DataClient interface for local filesystem I/O.
    """

    def get_object(self, path: str) -> BinaryIO:
        try:
            return open(path, "rb")
        except FileNotFoundError:
            raise FileNotFoundError(f"Local file not found: {path}")

    def put_object(self, path: str, data: BinaryIO, content_type: str = "application/octet-stream"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data.read())