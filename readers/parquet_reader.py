import pandas as pd
from typing import Optional, List, BinaryIO

class ParquetReader:
    """
    A reader class for loading Parquet files using a storage client.

    Parameters
    ----------
    client : object
        A storage client that provides a `get_object(path: str) -> BinaryIO` method
        to retrieve files from the storage backend.

    Methods
    -------
    read(path: str, columns: Optional[List[str]] = None, engine: str = 'auto') -> pd.DataFrame
        Reads a Parquet file from the specified path using the provided client.

    Examples
    --------
    >>> client = MinioDataClient(minio_client, 'my-bucket')
    >>> reader = ParquetReader(client)
    >>> df = reader.read('data/sample.parquet')
    >>> df.head()
    """

    def __init__(self, client):
        """
        Initialize the ParquetReader with a storage client.

        Parameters
        ----------
        client : object
            A storage client instance that implements the `get_object` method.
        """
        self._client = client

    def read(self, path: str, columns: Optional[List[str]] = None, engine: str = 'pyarrow') -> pd.DataFrame:
        """
        Read a Parquet file from the storage backend.

        Parameters
        ----------
        path : str
            The path to the Parquet file within the storage system.
        columns : list of str, optional
            List of column names to read from the file. Reads all columns if None.
        engine : {'auto', 'pyarrow', 'fastparquet'}, default 'auto'
            Parquet library to use. If 'auto', pandas will use the default engine.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data from the Parquet file.

        Raises
        ------
        FileNotFoundError
            If the specified file does not exist in the storage backend.
        ValueError
            If the file cannot be read as a Parquet file.
        """
        with self._client.get_object(path) as data:
            return pd.read_parquet(data, columns=columns, engine=engine)