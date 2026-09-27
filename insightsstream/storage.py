"""Object storage adapters shared by the pipeline and dashboard."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from insightsstream.config import Settings


class ObjectStore(ABC):
    @abstractmethod
    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        """Write bytes at an object key."""

    @abstractmethod
    def get_bytes(self, key: str) -> bytes:
        """Read bytes from an object key."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return whether an object exists."""

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]:
        """List keys under a prefix."""


class LocalObjectStore(ObjectStore):
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root not in path.parents and path != self.root:
            raise ValueError(f"Object key escapes the storage root: {key}")
        return path

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        del content_type
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if base.is_file():
            return [prefix]
        if not base.exists():
            return []
        return sorted(
            path.relative_to(self.root).as_posix()
            for path in base.rglob("*")
            if path.is_file()
        )


class R2ObjectStore(ObjectStore):
    def __init__(self, settings: Settings) -> None:
        import boto3
        from botocore.exceptions import ClientError

        settings.validate()
        assert settings.r2_bucket is not None
        self.bucket = settings.r2_bucket
        self._client_error = ClientError
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name="auto",
        )

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except self._client_error as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                return False
            raise
        return True

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        return [
            item["Key"]
            for page in pages
            for item in page.get("Contents", [])
        ]


def create_store(settings: Settings | None = None) -> ObjectStore:
    resolved = settings or Settings.from_env()
    resolved.validate()
    if resolved.storage_backend == "r2":
        return R2ObjectStore(resolved)
    return LocalObjectStore(resolved.local_lake_path)
