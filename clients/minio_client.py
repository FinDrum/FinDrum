from minio import Minio
from client import DataClient
from typing import BinaryIO


class MinioDataClient(DataClient):
    """
    Implements the DataClient interface for object storage using MinIO.

    This client enables reading and writing binary objects to a specified MinIO bucket.
    It verifies bucket existence before performing operations, ensuring reliability
    in data workflows.

    Parameters
    ----------
    client : Minio
        An initialized Minio client instance.
    bucket_name : str
        The name of the bucket to use for read/write operations.

    Methods
    -------
    get_object(path)
        Retrieves an object from the bucket as a binary stream.
    put_object(path, data, content_type)
        Stores a binary stream to the given path in the bucket.
    """

    def __init__(self, client: Minio, bucket_name: str):
        """
        Initialize the MinioDataClient.

        Parameters
        ----------
        client : Minio
            An instance of the Minio SDK client.
        bucket_name : str
            Name of the bucket to be used for all operations.
        """
        self._client = client
        self._bucket_name = bucket_name

    def get_object(self, path: str) -> BinaryIO:
        """
        Retrieve an object from the MinIO bucket.

        Parameters
        ----------
        path : str
            The object key within the bucket.

        Returns
        -------
        BinaryIO
            A stream of the object's binary data.

        Raises
        ------
        ValueError
            If the specified bucket does not exist.
        """
        self._ensure_bucket_exists()
        return self._client.get_object(self._bucket_name, path)

    def put_object(self, path: str, data: BinaryIO, content_type: str = "application/octet-stream"):
        """
        Upload a binary stream to the MinIO bucket at the given path.

        Parameters
        ----------
        path : str
            The object key under which the file will be stored.
        data : BinaryIO
            A binary stream to be written to the bucket.
        content_type : str, optional
            The MIME type of the object (default is "application/octet-stream").

        Raises
        ------
        ValueError
            If the specified bucket does not exist.
        """
        self._ensure_bucket_exists()
        self._client.put_object(
            bucket_name=self._bucket_name,
            object_name=path,
            data=data,
            length=data.getbuffer().nbytes,
            content_type=content_type,
        )

    def _ensure_bucket_exists(self):
        """
        Verify that the target bucket exists in MinIO.

        Raises
        ------
        ValueError
            If the bucket does not exist.
        """
        if not self._client.bucket_exists(self._bucket_name):
            raise ValueError(f"Bucket '{self._bucket_name}' does not exist in MinIO.")