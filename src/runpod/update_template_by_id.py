"""
Update a RunPod template using the REST API.

The RunPod SDK doesn't provide an update_template method,
so this script uses the REST API directly.
"""

import os
import sys
import argparse
import logging
import json
from typing import Optional, Dict, Any
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RUNPOD_API_BASE = os.environ.get("RUNPOD_API_BASE_URL")

def update_template(
    template_id: str,
    name: Optional[str] = None,
    image_name: Optional[str] = None,
    container_disk_in_gb: Optional[int] = None,
    volume_in_gb: Optional[int] = None,
    volume_mount_path: Optional[str] = None,
    docker_start_cmd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    ports: Optional[str] = None,
    is_public: Optional[bool] = None,
    api_key: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Update a RunPod template using the REST API.
    
    Args:
        template_id: The template ID to update
        name: Template name
        image_name: Docker image name (e.g., "username/image:tag")
        container_disk_in_gb: Container disk size in GB
        volume_in_gb: Volume size in GB
        volume_mount_path: Path where volume should be mounted
        docker_start_cmd: Docker start command
        env: Environment variables as a dictionary
        ports: Port mappings (e.g., "8888/http,22/tcp")
        is_public: Whether the template is public
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
        **kwargs: Additional parameters to pass to the API
    
    Returns:
        dict: Response from RunPod API
    
    Raises:
        ValueError: If API key is not provided
        requests.exceptions.RequestException: If API request fails
    """
    # Get API key from environment if not provided
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError(
                "API key must be provided or RUNPOD_API_KEY "
                "environment variable must be set"
            )
    
    url = f"{RUNPOD_API_BASE}/templates/{template_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Build payload with only provided parameters
    payload = {}
    
    if name is not None:
        payload["name"] = name
    if image_name is not None:
        payload["imageName"] = image_name
    if container_disk_in_gb is not None:
        payload["containerDiskInGb"] = container_disk_in_gb
    if volume_in_gb is not None:
        payload["volumeInGb"] = volume_in_gb
    if volume_mount_path is not None:
        payload["volumeMountPath"] = volume_mount_path
    if docker_start_cmd is not None:
        payload["dockerStartCmd"] = docker_start_cmd if isinstance(docker_start_cmd, list) else [docker_start_cmd]
    if env is not None:
        payload["env"] = env
    if ports is not None:
        payload["ports"] = ports
    if is_public is not None:
        payload["isPublic"] = is_public
    
    # Add any additional kwargs
    payload.update(kwargs)
    
    logger.info(f"Updating template {template_id}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.patch(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Template updated successfully: {template_id}")
        return result
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error occurred: {e}")
        logger.error(f"Response: {e.response.text}")
        raise
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update template: {e}")
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Update a RunPod template using the REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update template image:
  python update_template_by_id.py YOUR_TEMPLATE_ID --image username/image:latest
  
  # Update template name and resources:
  python update_template_by_id.py YOUR_TEMPLATE_ID \\
    --name "My Updated Template" \\
    --container-disk 30 \\
    --volume 100
  
  # Update with environment variables:
  python update_template_by_id.py YOUR_TEMPLATE_ID \\
    --image username/image:v2.0.0 \\
    --env AWS_REGION=us-east-1 \\
    --env LOG_LEVEL=debug
  
Environment Variables:
  RUNPOD_API_KEY - Your RunPod API key (required)
        """
    )
    
    parser.add_argument(
        "template_id",
        help="Template ID to update"
    )
    
    parser.add_argument(
        "--name",
        help="Template name"
    )
    
    parser.add_argument(
        "--image",
        dest="image_name",
        help="Docker image name (e.g., 'username/image:tag')"
    )
    
    parser.add_argument(
        "--container-disk",
        dest="container_disk_in_gb",
        type=int,
        help="Container disk size in GB"
    )
    
    parser.add_argument(
        "--volume",
        dest="volume_in_gb",
        type=int,
        help="Volume size in GB"
    )
    
    parser.add_argument(
        "--volume-mount-path",
        help="Volume mount path"
    )
    
    parser.add_argument(
        "--docker-start-cmd",
        help="Docker start command"
    )
    
    parser.add_argument(
        "--env",
        action="append",
        metavar="KEY=VALUE",
        help="Environment variables (can be specified multiple times)"
    )
    
    parser.add_argument(
        "--ports",
        help="Port mappings (e.g., '8888/http,22/tcp')"
    )
    
    parser.add_argument(
        "--public",
        dest="is_public",
        action="store_true",
        help="Make template public"
    )
    
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output response as JSON"
    )
    
    args = parser.parse_args()
    
    # Parse environment variables
    env_vars = {}
    if args.env:
        for env_pair in args.env:
            try:
                key, value = env_pair.split("=", 1)
                env_vars[key] = value
            except ValueError:
                logger.error(f"Invalid environment variable format: {env_pair}")
                sys.exit(1)
    
    try:
        # Build kwargs from args
        kwargs = {
            k: v for k, v in vars(args).items()
            if v is not None and k not in ['template_id', 'env', 'json']
        }
        
        if env_vars:
            kwargs['env'] = env_vars
        
        result = update_template(args.template_id, **kwargs)
        
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            logger.info("✓ Template updated successfully!")
            if isinstance(result, dict):
                logger.info(f"Template ID: {result.get('id', args.template_id)}")
                logger.info(f"Name: {result.get('name')}")
                logger.info(f"Image: {result.get('imageName')}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
