"""Lake storage: local filesystem or S3, plus Parquet/JSON table helpers."""

from __future__ import annotations

import io
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd

from common import PipelineError

# ---------------------------------------------------------------------------
# Storage (local filesystem or S3)
# ---------------------------------------------------------------------------


class LocalStore:
    """Object-store style access to a local directory."""

    def __init__(self, root: str) -> None:
        self.root = Path(root).expanduser()

    def _path(self, key: str) -> Path:
        return self.root / key

    def read_bytes(self, key: str) -> bytes:
        """Return the content of ``key``."""
        return self._path(key).read_bytes()

    def write_bytes(self, key: str, data: bytes) -> None:
        """Write ``data`` to ``key``, creating parent directories."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def exists(self, key: str) -> bool:
        """Return whether ``key`` exists."""
        return self._path(key).is_file()

    def list(self, prefix: str) -> list[str]:
        """Return all file keys under ``prefix``, sorted."""
        base = self._path(prefix) if prefix else self.root
        if not base.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())

    def delete_prefix(self, prefix: str) -> None:
        """Delete everything under ``prefix``."""
        shutil.rmtree(self._path(prefix), ignore_errors=True)

    def move_prefix(self, src: str, dst: str) -> None:
        """Move everything under ``src`` to ``dst``."""
        target = self._path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(self._path(src)), str(target))


class S3Store:
    """Object-store access to an S3 bucket and key prefix."""

    def __init__(self, uri: str) -> None:
        import boto3
        from botocore.config import Config as BotoConfig

        bucket, _, base = uri.removeprefix("s3://").partition("/")
        self.bucket = bucket
        self.base = base.strip("/")
        self.client = boto3.client("s3", config=BotoConfig(retries={"mode": "adaptive", "max_attempts": 5}))

    def _key(self, key: str) -> str:
        return f"{self.base}/{key}" if self.base else key

    def read_bytes(self, key: str) -> bytes:
        """Return the content of ``key``."""
        return self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"].read()

    def write_bytes(self, key: str, data: bytes) -> None:
        """Write ``data`` to ``key``."""
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def exists(self, key: str) -> bool:
        """Return whether ``key`` exists."""
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self._key(key), MaxKeys=1)
        return any(obj["Key"] == self._key(key) for obj in response.get("Contents", []))

    def list(self, prefix: str) -> list[str]:
        """Return all object keys under ``prefix``, sorted."""
        full = self._key(prefix).rstrip("/") + "/" if prefix else (f"{self.base}/" if self.base else "")
        strip = len(self.base) + 1 if self.base else 0
        keys = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=full):
            keys.extend(obj["Key"][strip:] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete_prefix(self, prefix: str) -> None:
        """Delete everything under ``prefix``."""
        keys = [self._key(k) for k in self.list(prefix)]
        for start in range(0, len(keys), 1000):
            batch = [{"Key": k} for k in keys[start:start + 1000]]
            self.client.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})

    def move_prefix(self, src: str, dst: str) -> None:
        """Copy everything under ``src`` to ``dst``, then delete ``src``."""
        for key in self.list(src):
            target = dst + key[len(src):]
            self.client.copy_object(Bucket=self.bucket, Key=self._key(target),
                                    CopySource={"Bucket": self.bucket, "Key": self._key(key)})
        self.delete_prefix(src)


Store = LocalStore | S3Store


def open_store(root: str) -> Store:
    """Return an S3 store for ``s3://`` roots, otherwise a local one."""
    return S3Store(root) if root.startswith("s3://") else LocalStore(root)


def read_many(store: Store, keys: list[str]) -> list[bytes]:
    """Read several objects in parallel, preserving order."""
    with ThreadPoolExecutor(max_workers=16) as pool:
        return list(pool.map(store.read_bytes, keys))


def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to Parquet."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def replace_prefix(store: Store, prefix: str, files: dict[str, bytes], run_id: str) -> None:
    """Replace everything under ``prefix`` with ``files``.

    Files are written to a staging area first, so a failure while writing
    leaves the previous version in place.
    """
    staging = f"_staging/{run_id}/{prefix}"
    for name, data in files.items():
        store.write_bytes(f"{staging}/{name}", data)
    store.delete_prefix(prefix)
    store.move_prefix(staging, prefix)


def write_table(store: Store, prefix: str, df: pd.DataFrame, run_id: str) -> None:
    """Replace a single-file table."""
    replace_prefix(store, prefix, {"part-0000.parquet": to_parquet_bytes(df)}, run_id)


def read_table(store: Store, prefix: str) -> pd.DataFrame:
    """Read every Parquet file under ``prefix`` into one DataFrame."""
    keys = [k for k in store.list(prefix) if k.endswith(".parquet")]
    if not keys:
        raise PipelineError(f"No data found at {prefix}; run the previous stage first")
    frames = [pd.read_parquet(io.BytesIO(data)) for data in read_many(store, keys)]
    return pd.concat(frames, ignore_index=True)


def write_json(store: Store, key: str, payload: Any) -> None:
    """Write ``payload`` as pretty-printed JSON."""
    store.write_bytes(key, json.dumps(payload, indent=2, default=str).encode())


def read_json(store: Store, key: str, default: Any = None) -> Any:
    """Read a JSON object, or return ``default`` if it does not exist."""
    return json.loads(store.read_bytes(key)) if store.exists(key) else default
