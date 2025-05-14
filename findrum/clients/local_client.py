import os
from typing import BinaryIO
from io import BytesIO

from findrum.clients.client import DataClient


class LocalDataClient(DataClient):
    """
    Implements the DataClient interface for local file system access.

    This client allows reading and writing binary files on the local file system
    using the standard Python I/O APIs. It is useful for development, testing,
    or production workflows where local storage is sufficient.

    Methods
    -------
    get_object(path)
        Read and return a binary stream from the specified local path.
    put_object(path, data, content_type)
        Write a binary stream to the specified local path, creating directories if needed.
    """

    def get_object(self, path: str) -> BinaryIO:
        """
        Read a file from the local file system as a binary stream.

        Parameters
        ----------
        path : str
            Path to the file to be read.

        Returns
        -------
        BinaryIO
            A binary stream of the file contents.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        NotADirectoryError
            If the parent directory does not exist.
        """
        self._ensure_directory_exists(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        return open(path, "rb")

    def put_object(self, path: str, data: BinaryIO, content_type: str = "application/octet-stream"):
        """
        Write binary data to the local file system at the specified path.

        Parameters
        ----------
        path : str
            Path where the binary file will be saved.
        data : BinaryIO
            A binary stream containing the data to write.
        content_type : str, optional
            MIME type of the file (ignored in local storage, included for API compatibility).

        Returns
        -------
        None
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data.read())

    def _ensure_directory_exists(self, path: str) -> None:
        """
        Check if the parent directory of the given path exists.

        Parameters
        ----------
        path : str
            File path whose parent directory is to be checked.

        Raises
        ------
        NotADirectoryError
            If the directory does not exist.
        """
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            raise NotADirectoryError(f"Directory does not exist: {directory}")