# RunPod SeedVR2 Video Upscaler

A serverless video upscaling solution using SeedVR2 on RunPod.

## Overview

This project provides a RunPod serverless handler for video upscaling using the SeedVR2 model. It includes:

- Docker image with CUDA support and SeedVR2 dependencies
- RunPod serverless handler for job processing
- Automated CI/CD for building and pushing Docker images
- Scripts for creating RunPod templates and endpoints

## Project Structure

```
.
├── src/
│   ├── upscale/
│   │   ├── Dockerfile          # Docker image with SeedVR2 and dependencies
│   │   ├── handler.py          # RunPod serverless handler
│   │   ├── requirements.txt    # Python dependencies
│   │   └── upscale_segment.sh  # Shell script for video processing
│   └── runpod/
│       ├── create_template.py  # Script to create RunPod template
│       └── create_endpoint.py  # Script to create RunPod endpoint
└── .github/
    └── workflows/
        └── docker-build-push.yml  # CI/CD for Docker image
```

## Setup

### Prerequisites

- RunPod account with API key
- DockerHub account (for hosting the Docker image)
- Python 3.8+

### Environment Variables

Set the following environment variables:

```bash
export RUNPOD_API_KEY="your-runpod-api-key"
export DOCKERHUB_USERNAME="your-dockerhub-username"
```

### Install Dependencies

```bash
pip install runpod
```

## Usage

### 1. Build and Push Docker Image

The GitHub Actions workflow automatically builds and pushes the Docker image when you push to the `main` branch or create a tag.

**Manual build:**

```bash
cd src/upscale
docker build -t $DOCKERHUB_USERNAME/seedvr2-upscaler:latest .
docker push $DOCKERHUB_USERNAME/seedvr2-upscaler:latest
```

### 2. Create RunPod Template

After your Docker image is pushed, create a RunPod template:

```bash
python src/runpod/create_template.py \
  --name "SeedVR2 Video Upscaler" \
  --image seedvr2-upscaler \
  --tag latest
```

**With custom configuration:**

```bash
python src/runpod/create_template.py \
  --name "SeedVR2 Video Upscaler" \
  --image seedvr2-upscaler \
  --tag v1.0.0 \
  --container-disk 30 \
  --volume 100 \
  --env AWS_REGION=us-east-1
```

**Update existing template:**

```bash
python src/runpod/create_template.py \
  --template-id YOUR_TEMPLATE_ID \
  --tag latest
```

### 3. Create RunPod Endpoint

(See `create_endpoint.py` for endpoint creation)

## Docker Image

The Docker image includes:

- NVIDIA CUDA 13.1.0 runtime
- Python 3 with virtual environment
- PyTorch with CUDA support
- SeedVR2 video upscaler
- AWS CLI for S3 operations
- RunPod SDK

### Image Tags

- `latest` - Latest build from main branch
- `main-<sha>` - Specific commit from main branch
- `v*.*.*` - Semantic version tags

## Serverless Handler

The handler accepts jobs with the following input format:

```json
{
  "input_s3_uri": "s3://bucket/path/to/input.mp4",
  "output_s3_uri": "s3://bucket/path/to/output.mp4",
  "params": {
    "model": "7b",
    "resolution": 1080,
    "batch_size": 129,
    "seed": 42,
    "models_s3_prefix": "s3://bucket/models/"
  }
}
```

## CI/CD

The project uses a modular GitHub Actions pipeline with three workflows:

### Workflow Architecture

1. **[deploy.yml](.github/workflows/deploy.yml)** - Parent workflow that orchestrates the entire deployment
   - Triggers on push to `main`, version tags, or changes to source/workflow files
   - Calls the Docker build workflow
   - Then calls the RunPod template workflow
   - Supports manual dispatch with custom parameters

2. **[docker-build-push.yml](.github/workflows/docker-build-push.yml)** - Reusable workflow for Docker operations
   - Builds the Docker image with SeedVR2 and dependencies
   - Pushes to DockerHub
   - Returns the image tag and digest as outputs
   - Can be called independently or as part of the parent workflow

3. **[runpod-template.yml](.github/workflows/runpod-template.yml)** - Reusable workflow for RunPod management
   - Creates or updates RunPod templates
   - Uses the image tag from the build workflow
   - Can be called independently or as part of the parent workflow

### Workflow Triggers

**Automated triggers (via deploy.yml):**

- Push to `main` branch
- Version tags (`v*.*.*`)
- Changes to `src/upscale/**`, `src/runpod/**`, or `.github/workflows/**`

**Manual triggers:**

- Each workflow can be triggered independently via workflow_dispatch
- Parent workflow supports custom Docker tags and SeedVR2 versions

### Automated Template Management

After each successful Docker image build and push, the workflow automatically creates or updates your RunPod template with the latest image tag.

**First-time setup:**

1. Push to `main` or manually trigger the `deploy.yml` workflow
2. The workflow creates a new template automatically
3. Check the "Create/Update RunPod Template" job logs for the template ID
4. Add it as `RUNPOD_TEMPLATE_ID` secret in your repository settings

**Subsequent runs:**

- If `RUNPOD_TEMPLATE_ID` is set, updates the existing template
- If not set, creates a new template each time

### Secrets Required

Set these in your GitHub repository settings (Settings → Secrets and variables → Actions):

- `DOCKERHUB_USERNAME` - Your DockerHub username
- `DOCKERHUB_TOKEN` - Your DockerHub access token
- `RUNPOD_API_KEY` - Your RunPod API key (required for template creation)
- `RUNPOD_TEMPLATE_ID` - (Optional) Existing template ID to update instead of creating new

### Running Individual Workflows

You can run each workflow independently:

**Build Docker image only:**

```
Actions → Build and Push Docker Image → Run workflow
```

**Update RunPod template only:**

```
Actions → Create or Update RunPod Template → Run workflow
(Requires: image_name, image_tag, dockerhub_username)
```

**Full deployment:**

```
Actions → Build, Push, and Deploy → Run workflow
```

## Development

### Local Testing

Build the Docker image locally:

```bash
cd src/upscale
docker build -t seedvr2-upscaler:dev .
```

Run the handler locally:

```bash
docker run --gpus all \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  seedvr2-upscaler:dev
```

## License

[Add your license here]
