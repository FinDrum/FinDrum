import io
import logging
from typing import Final

import pandas as pd

from writer import Writer
from ..clients.client import DataClient


class ParquetWriter(Writer):
    """
    Writes a pandas DataFrame to a Parquet file using a generic DataClient backend.

    This writer uploads the serialized Parquet data to the specified path via a user-provided
    DataClient implementation (e.g., for local filesystem, cloud storage, or object storage APIs).

    Parameters
    ----------
    client : DataClient
        An implementation of the DataClient interface responsible for handling the underlying
        I/O operations (e.g., file system, MinIO, S3).

    Notes
    -----
    - Empty DataFrames are skipped and trigger a warning.
    - Existing files at the target path are overwritten without confirmation.
    - Parquet serialization uses pandas with the default engine (`pyarrow` or `fastparquet`).

    Examples
    --------
    >>> client = LocalDataClient()
    >>> writer = ParquetWriter(client)
    >>> df = pd.DataFrame({"x": [1, 2, 3]})
    >>> writer.write("data/output/file.parquet", df)
    """

    _CONTENT_TYPE: Final[str] = "application/octet-stream"

    def __init__(self, client: DataClient):
        self._client = client

    def write(self, path: str, data: pd.DataFrame) -> None:
        """
        Write a pandas DataFrame to a Parquet file at the given path.

        Parameters
        ----------
        path : str
            The destination path (e.g., file path or object key) where the Parquet file
            will be written.
        data : pandas.DataFrame
            The DataFrame to serialize and upload.

        Returns
        -------
        None
        """
        if data.empty:
            logging.warning("ParquetWriter skipped empty DataFrame (%s)", path)
            return

        buffer = io.BytesIO()
        data.to_parquet(buffer, engine="fastparquet", index=False)
        buffer.seek(0)

        logging.debug("Uploading Parquet to %s", path)
        self._client.put_object(
            path=path,
            data=buffer,
            content_type=self._CONTENT_TYPE,
        )