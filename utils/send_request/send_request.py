#!/usr/bin/env python3
"""
Generate presigned URLs for input/output/models and invoke a RunPod endpoint.

Requirements:
  pip install boto3 runpod
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
import runpod
import yaml


def _assume_role(role_arn: str, session_name: str, region: str) -> boto3.session.Session:
    sts = boto3.client("sts", region_name=region)
    resp = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    creds = resp["Credentials"]
    return boto3.session.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


def _get_s3_client(region: str, role_arn: Optional[str], session_name: str):
    if role_arn:
        session = _assume_role(role_arn, session_name, region)
        return session.client("s3")
    return boto3.client("s3", region_name=region)


def _load_params(
    params_json: Optional[str],
    params_file: Optional[str],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    if params_json and params_file:
        raise ValueError("Use either --params-json or --params-file, not both.")
    if params_file:
        with open(params_file, "r", encoding="utf-8") as handle:
            return json.load(handle)
    if params_json:
        return json.loads(params_json)
    if "params" in config:
        return config["params"]
    if "params_json" in config:
        return json.loads(config["params_json"])
    if "params_file" in config:
        with open(config["params_file"], "r", encoding="utf-8") as handle:
            return json.load(handle)
    return {}


def _presign_get(s3_client, bucket: str, key: str, expires: int) -> str:
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


def _presign_put(s3_client, bucket: str, key: str, expires: int) -> str:
    return s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


def main() -> int:
    default_config_path = Path(__file__).resolve().parent / "config.local.yaml"
    parser = argparse.ArgumentParser(
        description="Presign S3 URLs and invoke a RunPod endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", help=f"Path to config file (default: {default_config_path})")
    parser.add_argument("--endpoint-id", help="RunPod endpoint ID")
    parser.add_argument(
        "--run-sync",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use runsync",
    )
    parser.add_argument("--timeout", type=int, help="Run sync timeout in seconds")
    parser.add_argument("--runpod-api-key", help="RunPod API key override")

    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--role-arn", help="Assume this role before presigning")
    parser.add_argument(
        "--role-session-name",
        help="Session name for assumed role",
    )
    parser.add_argument("--expires", type=int, help="Presigned URL expiration (seconds)")

    parser.add_argument("--input-bucket", help="S3 bucket for input video")
    parser.add_argument("--input-key", help="S3 key for input video")
    parser.add_argument("--output-bucket", help="S3 bucket for output video")
    parser.add_argument("--output-key", help="S3 key for output video")
    parser.add_argument("--vae-bucket", help="S3 bucket for VAE model (defaults to input bucket)")
    parser.add_argument("--vae-key", help="S3 key for VAE model")
    parser.add_argument("--dit-bucket", help="S3 bucket for DiT model (defaults to input bucket)")
    parser.add_argument("--dit-key", help="S3 key for DiT model")

    parser.add_argument("--params-json", help="Inline JSON for params")
    parser.add_argument("--params-file", help="Path to JSON file for params")

    args = parser.parse_args()

    config_path = Path(args.config) if args.config else default_config_path
    config: Dict[str, Any] = {}
    if args.config or config_path.exists():
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}

    endpoint_id = args.endpoint_id or config.get("endpoint_id")
    if not endpoint_id:
        raise ValueError("endpoint_id is required (argument or config)")

    region = args.region or config.get("region") or os.environ.get("AWS_REGION", "us-east-1")
    role_arn = args.role_arn or config.get("role_arn")
    role_session_name = (
        args.role_session_name
        or config.get("role_session_name")
        or "local-runpod-presign"
    )
    expires = args.expires or config.get("expires") or 3600

    input_bucket = args.input_bucket or config.get("input_bucket")
    input_key = args.input_key or config.get("input_key")
    output_bucket = args.output_bucket or config.get("output_bucket")
    output_key = args.output_key or config.get("output_key")
    vae_bucket = args.vae_bucket or config.get("vae_bucket") or input_bucket
    vae_key = args.vae_key or config.get("vae_key")
    dit_bucket = args.dit_bucket or config.get("dit_bucket") or input_bucket
    dit_key = args.dit_key or config.get("dit_key")

    missing = [
        name
        for name, value in [
            ("input_bucket", input_bucket),
            ("input_key", input_key),
            ("output_bucket", output_bucket),
            ("output_key", output_key),
            ("vae_key", vae_key),
            ("dit_key", dit_key),
        ]
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required values: {', '.join(missing)}")

    s3_client = _get_s3_client(region, role_arn, role_session_name)

    input_url = _presign_get(s3_client, input_bucket, input_key, expires)
    output_url = _presign_put(s3_client, output_bucket, output_key, expires)
    vae_url = _presign_get(s3_client, vae_bucket, vae_key, expires)
    dit_url = _presign_get(s3_client, dit_bucket, dit_key, expires)

    params = _load_params(args.params_json, args.params_file, config)

    payload = {
        "input_presigned_url": input_url,
        "output_presigned_url": output_url,
        "vae_model_presigned_url": vae_url,
        "dit_model_presigned_url": dit_url,
        "params": params
    }

    payload = {"input": payload}

    api_key = args.runpod_api_key or config.get("runpod_api_key") or os.environ.get("RUNPOD_API_KEY")
    if api_key:
        runpod.api_key = api_key

    run_sync = args.run_sync if args.run_sync is not None else config.get("run_sync", False)
    timeout = args.timeout or config.get("timeout") or 3600

    endpoint = runpod.Endpoint(endpoint_id)
    if run_sync:
        result = endpoint.run_sync(payload, timeout=timeout)
        print(json.dumps(result, indent=2))
    else:
        job = endpoint.run(payload)
        print(json.dumps({"job_id": job.job_id, "payload": payload}, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
