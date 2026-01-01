# upload_large_file

Reliable multipart uploader for large files (10GB+) to Runpod's Network Volume S3-compatible endpoints.

This script is based on this [RunPod example](https://github.com/runpod/runpod-s3-examples/blob/main/upload_large_file.py).

## Requirements

```bash
pip install -r requirements.txt
```

## Usage (CLI)

```bash
./upload_large_file.py \
  --file /path/to/large/file.mp4 \
  --bucket <NETWORK_VOLUME_ID> \
  --key uploads/file.mp4 \
  --endpoint https://s3api-eur-is-1.runpod.io/ \
  --region EUR-IS-1
```

Access key and secret key are required (via CLI, config, or environment variables).

Optional flags:

- `--chunk-size` (bytes, optional; auto-calculated if omitted)
- `--workers` (parallel uploads, default 4)
- `--max-retries` (default 5)
- `--content-type` (set on uploaded object)
- `--quiet`

## Usage (config.yaml)

Create a `config.yaml` next to the script:

```yaml
bucket: your-bucket
key: uploads/file.mp4
file: /path/to/large/file.mp4
endpoint: https://s3api-eur-is-1.runpod.io/
region: EUR-IS-1
access_key: YOUR_ACCESS_KEY_ID
secret_key: YOUR_SECRET_ACCESS_KEY
# chunk_size is optional; if omitted, it will be calculated automatically.
chunk_size: 52428800
workers: 4
max_retries: 5
content_type: video/mp4
```

Then run:

```bash
./upload_large_file.py
```

You can also point to a custom config location with `--config /path/to/config.yaml`.

## Environment Variables

The script also reads these if CLI/config are not provided:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `S3_ENDPOINT`
- `S3_REGION` (or `AWS_REGION`)
- `MAX_RETRIES`

## Notes

- Multipart uploads require part sizes >= 5MB and <= 5GB.
- Keep total parts <= 10,000; increase `chunk_size` for huge files.
