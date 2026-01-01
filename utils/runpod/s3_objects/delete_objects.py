#!/usr/bin/env python3
"""
Delete objects from a RunPod network volume (S3-compatible) bucket.
"""

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List

import boto3
from botocore.config import Config
import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
DELETE_BATCH_SIZE = 1000


def load_config(path: str | None) -> Dict[str, Any]:
    config_path = Path(path).expanduser() if path else DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def coalesce(*values):
    for value in values:
        if value is not None:
            return value
    return None


def parse_keys(value) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def read_keys_file(path: str | None) -> List[str]:
    if not path:
        return []
    file_path = Path(path).expanduser()
    if not file_path.exists():
        raise FileNotFoundError(f"Keys file not found: {file_path}")
    keys = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                keys.append(line)
    return keys


def build_client(
    *,
    region: str,
    endpoint: str | None,
    access_key: str,
    secret_key: str,
    max_retries: int,
):
    session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    cfg = Config(
        region_name=region,
        retries={"max_attempts": max_retries, "mode": "standard"},
    )
    return session.client("s3", config=cfg, endpoint_url=endpoint)


def iter_prefix_keys(s3, *, bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj.get("Key")


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def parse_args():
    parser = argparse.ArgumentParser(description="Delete objects from an S3 bucket")
    parser.add_argument("--config", help=f"Path to config YAML (default: {DEFAULT_CONFIG_PATH})")
    parser.add_argument("--bucket", help="S3 bucket name (network volume ID)")
    parser.add_argument("--prefix", help="Delete all objects with this prefix")
    parser.add_argument(
        "--keys",
        help="Comma-separated list of keys to delete",
    )
    parser.add_argument(
        "--keys-file",
        help="Path to a file containing keys to delete (one per line)",
    )
    parser.add_argument("--endpoint", help="S3 API endpoint URL")
    parser.add_argument("--region", help="S3 region name")
    parser.add_argument("--access-key", help="AWS access key")
    parser.add_argument("--secret-key", help="AWS secret key")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform deletion (otherwise dry-run)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        help="Max retries for S3 calls (default: 5)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-object output",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)

    bucket = coalesce(args.bucket, config.get("bucket"))
    prefix = coalesce(args.prefix, config.get("prefix"))
    keys = parse_keys(coalesce(args.keys, config.get("keys")))
    keys_file = coalesce(args.keys_file, config.get("keys_file"))
    keys.extend(read_keys_file(keys_file))

    region = coalesce(
        args.region,
        config.get("region"),
        os.environ.get("S3_REGION"),
        os.environ.get("AWS_REGION"),
    )
    endpoint = coalesce(args.endpoint, config.get("endpoint"), os.environ.get("S3_ENDPOINT"))
    access_key = coalesce(
        args.access_key, config.get("access_key"), os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret_key = coalesce(
        args.secret_key, config.get("secret_key"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    max_retries = coalesce(args.max_retries, config.get("max_retries"))
    if max_retries is None:
        max_retries = int(os.environ.get("MAX_RETRIES", 5))

    missing = [
        name
        for name, value in [("bucket", bucket), ("region", region)]
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required values: {', '.join(missing)}")
    if not access_key or not secret_key:
        raise ValueError(
            "access_key and secret_key are required (set via config, CLI, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)"
        )

    s3 = build_client(
        region=region,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        max_retries=max_retries,
    )

    if prefix:
        keys.extend(iter_prefix_keys(s3, bucket=bucket, prefix=prefix))

    deduped_keys = []
    seen = set()
    for key in keys:
        if key and key not in seen:
            deduped_keys.append(key)
            seen.add(key)

    if not deduped_keys:
        raise ValueError("No keys to delete (provide --prefix, --keys, or --keys-file)")

    if not args.confirm:
        print("Dry run: use --confirm to delete these objects.")
        if not args.quiet:
            for key in deduped_keys:
                print(key)
        print(f"Total objects matched: {len(deduped_keys)}")
        return 0

    deleted = 0
    for batch in chunked(deduped_keys, DELETE_BATCH_SIZE):
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        deleted += len(batch)
        errors = resp.get("Errors", [])
        if errors:
            for error in errors:
                print(f"Failed to delete {error.get('Key')}: {error.get('Message')}")

    if not args.quiet:
        print(f"Deleted objects: {deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
