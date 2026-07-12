"""
MinIO utility functions for Airflow DAGs.
Provides helpers for interacting with MinIO/S3 storage.
"""
import os
import logging
from minio import Minio
from minio.error import S3Error

log = logging.getLogger(__name__)

# Default configuration from environment
DEFAULT_MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
DEFAULT_MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
DEFAULT_MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
DEFAULT_MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
DEFAULT_RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw")


def get_minio_client(
    endpoint: str = DEFAULT_MINIO_ENDPOINT,
    access_key: str = DEFAULT_MINIO_ACCESS_KEY,
    secret_key: str = DEFAULT_MINIO_SECRET_KEY,
    secure: bool = DEFAULT_MINIO_SECURE,
) -> Minio:
    """
    Create and return a MinIO client.

    Args:
        endpoint: MinIO endpoint (host:port)
        access_key: Access key
        secret_key: Secret key
        secure: Use HTTPS if True

    Returns:
        Configured Minio client instance
    """
    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def ensure_bucket(client: Minio, bucket: str) -> bool:
    """
    Ensure a bucket exists, create if not.

    Args:
        client: MinIO client
        bucket: Bucket name

    Returns:
        True if bucket exists or was created, False on error
    """
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            log.info(f"Created bucket: {bucket}")
        else:
            log.info(f"Bucket exists: {bucket}")
        return True
    except S3Error as e:
        log.error(f"Failed to ensure bucket {bucket}: {e}")
        return False


def list_objects(client: Minio, bucket: str, prefix: str = "", recursive: bool = True) -> list:
    """
    List objects in a bucket with optional prefix.

    Args:
        client: MinIO client
        bucket: Bucket name
        prefix: Object prefix filter
        recursive: List recursively

    Returns:
        List of object info dicts
    """
    try:
        objects = []
        for obj in client.list_objects(bucket, prefix=prefix, recursive=recursive):
            objects.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                "etag": obj.etag,
                "content_type": obj.content_type,
            })
        return objects
    except S3Error as e:
        log.error(f"Error listing objects in {bucket}/{prefix}: {e}")
        return []


def get_object(client: Minio, bucket: str, object_name: str) -> bytes | None:
    """
    Download an object and return its content as bytes.

    Args:
        client: MinIO client
        bucket: Bucket name
        object_name: Object name/key

    Returns:
        Object content as bytes, or None on error
    """
    try:
        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except S3Error as e:
        log.error(f"Error getting object {bucket}/{object_name}: {e}")
        return None


def put_object(
    client: Minio,
    bucket: str,
    object_name: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> bool:
    """
    Upload bytes data to MinIO.

    Args:
        client: MinIO client
        bucket: Bucket name
        object_name: Object name/key
        data: Bytes data to upload
        content_type: MIME type

    Returns:
        True on success, False on error
    """
    try:
        from io import BytesIO
        ensure_bucket(client, bucket)
        data_stream = BytesIO(data)
        client.put_object(
            bucket,
            object_name,
            data_stream,
            len(data),
            content_type=content_type,
        )
        log.info(f"Uploaded {len(data)} bytes to {bucket}/{object_name}")
        return True
    except S3Error as e:
        log.error(f"Error putting object {bucket}/{object_name}: {e}")
        return False


def delete_object(client: Minio, bucket: str, object_name: str) -> bool:
    """
    Delete an object from MinIO.

    Args:
        client: MinIO client
        bucket: Bucket name
        object_name: Object name/key

    Returns:
        True on success, False on error
    """
    try:
        client.remove_object(bucket, object_name)
        log.info(f"Deleted object {bucket}/{object_name}")
        return True
    except S3Error as e:
        log.error(f"Error deleting object {bucket}/{object_name}: {e}")
        return False


def check_minio_health(endpoint: str = DEFAULT_MINIO_ENDPOINT) -> bool:
    """
    Check MinIO connectivity.

    Args:
        endpoint: MinIO endpoint

    Returns:
        True if healthy, False otherwise
    """
    try:
        client = get_minio_client(endpoint=endpoint)
        client.list_buckets()
        return True
    except Exception as e:
        log.error(f"MinIO health check failed: {e}")
        return False