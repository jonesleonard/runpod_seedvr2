# Send Request Helper

This utility presigns S3 URLs for the input/output and model files, then invokes
a RunPod endpoint locally.

## Requirements

- Python 3.9+
- `pip install boto3 runpod`

If you plan to assume a role, your local AWS credentials must allow `sts:AssumeRole`.

## Usage

Run the script:

```bash
python utils/send_request/send_request.py \
  --endpoint-id YOUR_ENDPOINT_ID \
  --region us-east-1 \
  --role-arn arn:aws:iam::123456789012:role/YourRole \
  --input-bucket my-bucket --input-key input/segment.mp4 \
  --output-bucket my-bucket --output-key output/up.mp4 \
  --vae-key models/vae.safetensors \
  --dit-key models/dit.safetensors \
  --params-json '{"model":"7b","resolution":1080}'
```

To run async (return a job id), omit `--run-sync`. To run sync, add `--run-sync`
and optionally set `--timeout`.

## Parameters

- `--endpoint-id` (required): RunPod endpoint id.
- `--region`: AWS region (default: `us-east-1`, or `AWS_REGION` env var).
- `--role-arn`: Optional role to assume before presigning.
- `--role-session-name`: Role session name (default: `local-runpod-presign`).
- `--expires`: Presigned URL expiration in seconds (default: `3600`).
- `--input-bucket`/`--input-key`: S3 object for the input video.
- `--output-bucket`/`--output-key`: S3 object for the output video (PUT URL).
- `--vae-key`/`--dit-key`: S3 keys for the model files.
- `--vae-bucket`/`--dit-bucket`: Optional model buckets (defaults to input bucket).
- `--params-json` or `--params-file`: Optional inference params.

## Environment

- `RUNPOD_API_KEY`: RunPod API key (used by the client).
- `AWS_REGION`: Default region if `--region` is not provided.

## Config File

You can provide a local config file at `utils/send_request/config.local.yaml`
or pass a custom path with `--config`. Values in CLI args override config.

Example:

```yaml
endpoint_id: YOUR_ENDPOINT_ID
runpod_api_key: YOUR_RUNPOD_API_KEY
region: us-east-1
role_arn: arn:aws:iam::123456789012:role/YourRole
role_session_name: local-runpod-presign
expires: 3600
input_bucket: my-bucket
input_key: input/segment.mp4
output_bucket: my-bucket
output_key: output/up.mp4
vae_key: models/vae.safetensors
dit_key: models/dit.safetensors
params:
  model: "7b"
  resolution: 1080
run_sync: false
timeout: 3600
```
