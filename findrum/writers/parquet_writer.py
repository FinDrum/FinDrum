import io
import logging
from typing import Final, Optional, List

import pandas as pd

from findrum.writers.writer import Writer
from findrum.clients.client import DataClient


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
    >>> df = pd.DataFrame({"x": [3, 1, 2]})
    >>> writer.write("data/output/file.parquet", df, sort_by=["x"])
    """

    _CONTENT_TYPE: Final[str] = "application/octet-stream"

    def __init__(self, client: DataClient):
        self._client = client

    def write(
        self,
        path: str,
        data: pd.DataFrame,
        engine: str = "fastparquet",
        sort_by: Optional[List[str]] = None
    ) -> None:
        """
        Write a pandas DataFrame to a Parquet file at the given path.

        Parameters
        ----------
        path : str
            The destination path where the Parquet file will be stored.
        data : pd.DataFrame
            The DataFrame to serialize and upload.
        engine : str, default "fastparquet"
            Parquet engine to use: 'pyarrow' or 'fastparquet'.
        sort_by : list of str, optional
            Columns to sort the DataFrame by before writing.

        Returns
        -------
        None
        """
        if data.empty:
            logging.warning("ParquetWriter skipped empty DataFrame (%s)", path)
            return

        if sort_by:
            missing = [col for col in sort_by if col not in data.columns]
            if missing:
                logging.warning("Sort skipped — columns not found in DataFrame: %s", missing)
            else:
                data = data.sort_values(by=sort_by)

        buffer = io.BytesIO()
        data.to_parquet(buffer, engine=engine, index=False)
        buffer.seek(0)

        logging.debug("Uploading Parquet to %s", path)
        self._client.put_object(
            path=path,
            data=buffer,
            content_type=self._CONTENT_TYPE,
        )
