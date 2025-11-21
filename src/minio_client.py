"""MinIO client for async operations."""

import asyncio
from minio import Minio
from minio.error import S3Error
import io
import structlog
from typing import Optional


logger = structlog.get_logger(__name__)


class MinioClient:
    """An async wrapper for the MinIO client."""

    def __init__(
        self, endpoint: str, access_key: str, secret_key: str, secure: bool = False
    ):
        """Initialize the MinIO client.

        Args:
            endpoint: The URL of the MinIO server (e.g., 'localhost:9000')
            access_key: The access key
            secret_key: The secret key
            secure: Whether to use HTTPS
        """
        try:
            self.client = Minio(
                endpoint, access_key=access_key, secret_key=secret_key, secure=secure
            )
            logger.info("MinIO client initialized", endpoint=endpoint)
        except Exception as e:
            logger.error("Failed to initialize MinIO client", error=str(e))
            raise

    async def ensure_bucket_exists(self, bucket_name: str):
        """Check if a bucket exists and create it if it does not.

        Args:
            bucket_name: Name of the bucket
        """
        try:
            found = await asyncio.to_thread(self.client.bucket_exists, bucket_name)
            if not found:
                await asyncio.to_thread(self.client.make_bucket, bucket_name)
                logger.info("Created MinIO bucket", bucket_name=bucket_name)
            else:
                logger.info("MinIO bucket already exists", bucket_name=bucket_name)
        except S3Error as e:
            logger.error(
                "Error checking/creating MinIO bucket",
                bucket_name=bucket_name,
                error=str(e),
            )
            raise

    async def get_object(self, bucket_name: str, object_name: str) -> Optional[bytes]:
        """Download an object from a bucket.

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object (its path/key)

        Returns:
            bytes: The bytes of the downloaded object, or None if failed
        """
        try:

            def _read_object():
                response = self.client.get_object(bucket_name, object_name)
                try:
                    data = response.read()
                    return data
                finally:
                    response.close()
                    response.release_conn()

            data = await asyncio.to_thread(_read_object)
            logger.debug(
                "Successfully downloaded object from MinIO",
                bucket=bucket_name,
                key=object_name,
            )
            return data
        except S3Error as e:
            logger.error(
                "Failed to download object from MinIO",
                bucket=bucket_name,
                key=object_name,
                error=str(e),
            )
            return None
        except Exception as e:
            logger.error(
                "Unexpected error downloading object from MinIO",
                bucket=bucket_name,
                key=object_name,
                error=str(e),
            )
            return None

    async def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        length: int,
        content_type: str = "application/octet-stream",
    ):
        """Upload an object to a bucket.

        Args:
            bucket_name: The name of the bucket
            object_name: The name of the object (its path/key)
            data: The object data in bytes
            length: The length of the data
            content_type: The MIME type of the content
        """
        try:
            await asyncio.to_thread(
                self.client.put_object,
                bucket_name,
                object_name,
                io.BytesIO(data),
                length,
                content_type=content_type,
            )
            logger.debug(
                "Successfully uploaded object to MinIO",
                bucket=bucket_name,
                key=object_name,
            )
        except S3Error as e:
            logger.error(
                "Failed to upload object to MinIO",
                bucket=bucket_name,
                key=object_name,
                error=str(e),
            )
