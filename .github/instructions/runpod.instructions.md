---
description: 'RunPod serverless development guidelines including handler patterns, job processing, and SDK best practices'
applyTo: 'src/runpod/**,src/**/handler.py'
---

# RunPod Serverless Development Guide

Comprehensive instructions for developing serverless functions and handlers for the RunPod platform, covering API key management, handler patterns, job processing, error handling, and optimization.

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

## SDK Usage Patterns

### Templates

#### Create a new Template

### Source Code Reference

```python
def create_template(
    name: str,
    image_name: str,
    docker_start_cmd: str = None,
    container_disk_in_gb: int = 10,
    volume_in_gb: int = None,
    volume_mount_path: str = None,
    ports: str = None,
    env: dict = None,
    is_serverless: bool = False,
    registry_auth_id: str = None,
):
    """
    Create a template

    :param name: the name of the template
    :param image_name: the name of the docker image to be used by the template
    :param docker_start_cmd: the command to start the docker container with
    :param container_disk_in_gb: how big should the container disk be
    :param volume_in_gb: how big should the volume be
    :param ports: the ports to open in the pod, example format - "8888/http,666/tcp"
    :param volume_mount_path: where to mount the volume?
    :param env: the environment variables to inject into the pod,
                for example {EXAMPLE_VAR:"example_value", EXAMPLE_VAR2:"example_value 2"}, will
                inject EXAMPLE_VAR and EXAMPLE_VAR2 into the pod with the mentioned values
    :param is_serverless: is the template serverless?
    :param registry_auth_id: the id of the registry auth

    :example:

    >>> template_id = runpod.create_template("test", "runpod/stack", "python3 main.py")
    """
    raw_response = run_graphql_query(
        template_mutations.generate_pod_template(
            name=name,
            image_name=image_name,
            docker_start_cmd=docker_start_cmd,
            container_disk_in_gb=container_disk_in_gb,
            volume_in_gb=volume_in_gb,
            volume_mount_path=volume_mount_path,
            ports=ports,
            env=env,
            is_serverless=is_serverless,
            registry_auth_id=registry_auth_id,
        )
    )

    return raw_response["data"]["saveTemplate"]
```

## Additional Resources

- [RunPod Documentation](https://docs.runpod.io/)
- [RunPod Python SDK](https://github.com/runpod/runpod-python)
- [Serverless Best Practices](https://docs.runpod.io/serverless/overview)