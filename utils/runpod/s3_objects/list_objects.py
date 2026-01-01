#!/usr/bin/env python3
"""
List objects in a RunPod network volume (S3-compatible) bucket.
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable

import boto3
from botocore.config import Config
import yaml


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


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


def iter_objects(
    s3,
    *,
    bucket: str,
    prefix: str | None,
    page_size: int,
    max_items: int | None,
) -> Iterable[dict]:
    paginator = s3.get_paginator("list_objects_v2")
    pagination_cfg = {"PageSize": page_size}
    if max_items:
        pagination_cfg["MaxItems"] = max_items
    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix or "",
        PaginationConfig=pagination_cfg,
    ):
        for obj in page.get("Contents", []):
            yield obj


def parse_args():
    parser = argparse.ArgumentParser(description="List objects in an S3 bucket")
    parser.add_argument("--config", help=f"Path to config YAML (default: {DEFAULT_CONFIG_PATH})")
    parser.add_argument("--bucket", help="S3 bucket name (network volume ID)")
    parser.add_argument("--prefix", help="Prefix filter")
    parser.add_argument("--endpoint", help="S3 API endpoint URL")
    parser.add_argument("--region", help="S3 region name")
    parser.add_argument("--access-key", help="AWS access key")
    parser.add_argument("--secret-key", help="AWS secret key")
    parser.add_argument(
        "--page-size",
        type=int,
        help="Page size for listing (default: 1000)",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        help="Stop after returning this many objects",
    )
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument(
        "--max-retries",
        type=int,
        help="Max retries for S3 calls (default: 5)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(args.config)

    bucket = coalesce(args.bucket, config.get("bucket"))
    prefix = coalesce(args.prefix, config.get("prefix"))
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
    page_size = coalesce(args.page_size, config.get("page_size")) or 1000
    max_items = coalesce(args.max_items, config.get("max_items"))
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

    items = []
    total_bytes = 0
    for obj in iter_objects(
        s3,
        bucket=bucket,
        prefix=prefix,
        page_size=page_size,
        max_items=max_items,
    ):
        total_bytes += obj.get("Size", 0)
        items.append(
            {
                "key": obj.get("Key"),
                "size": obj.get("Size"),
                "last_modified": obj.get("LastModified").isoformat(),
            }
        )

    if args.json:
        print(json.dumps(items, indent=2))
    else:
        for obj in items:
            print(f"{obj['key']}\t{obj['size']}\t{obj['last_modified']}")
        print(f"Total objects: {len(items)}")
        print(f"Total bytes: {total_bytes}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
