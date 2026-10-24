"""S3-compatible object storage for the landing and validated buckets."""
from __future__ import annotations

import json
import time
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from .config import Settings


def client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def ensure_buckets(settings: Settings, wait_seconds: int = 0) -> None:
    s3 = client(settings)
    deadline = time.time() + wait_seconds
    while True:
        try:
            existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
            break
        except Exception:  # the store may still be starting
            if time.time() >= deadline:
                raise
            time.sleep(2)
    for name in (settings.landing_bucket, settings.validated_bucket):
        if name not in existing:
            s3.create_bucket(Bucket=name)


def exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_file(s3, bucket: str, key: str, path: Path) -> None:
    s3.upload_file(str(path), bucket, key)


def get_file(s3, bucket: str, key: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(path))


def put_json(s3, bucket: str, key: str, obj: dict) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj, indent=2, sort_keys=True).encode(),
                  ContentType="application/json")


def get_json(s3, bucket: str, key: str) -> dict:
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
