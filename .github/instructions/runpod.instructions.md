---
description: 'RunPod serverless development guidelines including handler patterns, job processing, and SDK best practices'
applyTo: 'src/runpod_mgmt/**,src/**/handler.py'
---

# RunPod Serverless Development Guide

Comprehensive instructions for developing serverless functions and handlers for the RunPod platform, covering API key management, handler patterns, job processing, error handling, and optimization.

use the "#tool:runpod-documentation/SearchRunpodDocumentation" for looking up documentation and guidelines when needed.

## Project Context

- **Target Platform**: RunPod Serverless
- **Primary SDK**: `runpod` Python package (version 1.8.1+)

## General Instructions

- Use RunPod serverless for GPU-accelerated workloads and scalable processing tasks
- Design handlers to be stateless and idempotent for reliable execution
- Implement comprehensive error handling and logging for debugging in production
- Optimize for cold start times by minimizing dependencies and lazy-loading heavy resources
- Use environment variables for configuration, never hardcode credentials or URIs

## API Key Management

### Setting API Key

Set the RunPod API key globally at the start of your script for authentication:

```python
import runpod
import os

# Set API key from environment variable
runpod.api_key = os.environ.get("RUNPOD_API_KEY")
```

## Additional Resources

- [RunPod Documentation](https://docs.runpod.io/)
- [RunPod Python SDK](https://github.com/runpod/runpod-python)
- [Serverless Best Practices](https://docs.runpod.io/serverless/overview)