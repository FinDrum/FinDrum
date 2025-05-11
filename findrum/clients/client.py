from abc import ABC, abstractmethod
from typing import BinaryIO

class DataClient(ABC):
    @abstractmethod
    def get_object(self, path: str) -> BinaryIO:
        """Read a file and return it as binary stream."""
        pass # pragma: no cover

    @abstractmethod
    def put_object(self, path: str, data: BinaryIO, content_type: str = "application/octet-stream"):
        """Write a binary stream to the specified path."""
        pass # pragma: no cover