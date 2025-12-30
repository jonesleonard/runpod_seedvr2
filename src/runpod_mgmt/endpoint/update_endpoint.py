"""
Update a RunPod endpoint using the REST API.

The RunPod SDK doesn't have an update_endpoint function that allows
updating the template, so we use the REST API directly.
"""

import os
import logging
import requests
from typing import Optional, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def update_endpoint(
    endpoint_id: str,
    template_id: str,
    api_key: Optional[str] = None,
    name: Optional[str] = None,
    gpu_ids: Optional[str] = None,
    workers_min: Optional[int] = None,
    workers_max: Optional[int] = None,
    idle_timeout: Optional[int] = None,
    execution_timeout_ms: Optional[int] = None,
    scaler_type: Optional[str] = None,
    scaler_value: Optional[int] = None
) -> Dict[str, Any]:
    """
    Update an existing endpoint using the REST API.
    
    Args:
        endpoint_id: ID of the endpoint to update
        template_id: New template ID to use
        api_key: RunPod API key (if not provided, uses RUNPOD_API_KEY env var)
        name: New endpoint name (optional)
        gpu_ids: GPU type IDs (optional)
        workers_min: Minimum number of workers (optional)
        workers_max: Maximum number of workers (optional)
        idle_timeout: Idle timeout in seconds (optional)
        execution_timeout_ms: Execution timeout in milliseconds (optional)
        scaler_type: Scaler type (e.g., "QUEUE_DELAY") (optional)
        scaler_value: Scaler value (optional)
    
    Returns:
        dict: Response from RunPod API
    """
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError("RUNPOD_API_KEY environment variable is required")
    
    # Get base URL from environment or use default
    base_url = os.environ.get("RUNPOD_REST_API_BASE_URL", "https://rest.runpod.io/v1")
    url = f"{base_url}/endpoints/{endpoint_id}"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Build update payload - only include provided parameters
    payload = {
        "templateId": template_id
    }
    
    if name is not None:
        payload["name"] = name
    if gpu_ids is not None:
        payload["gpuTypeIds"] = [gpu_ids] if isinstance(gpu_ids, str) else gpu_ids
    if workers_min is not None:
        payload["workersMin"] = workers_min
    if workers_max is not None:
        payload["workersMax"] = workers_max
    if idle_timeout is not None:
        payload["idleTimeout"] = idle_timeout
    if execution_timeout_ms is not None:
        payload["executionTimeoutMs"] = execution_timeout_ms
    if scaler_type is not None:
        payload["scalerType"] = scaler_type
    if scaler_value is not None:
        payload["scalerValue"] = scaler_value
    
    logger.info(f"Updating endpoint {endpoint_id} with template {template_id}")
    logger.debug(f"Payload: {payload}")
    
    try:
        response = requests.patch(url, json=payload, headers=headers)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Endpoint updated successfully")
        return result
    
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error updating endpoint: {e}")
        logger.error(f"Response: {e.response.text}")
        raise
    except Exception as e:
        logger.error(f"Error updating endpoint: {e}")
        raise


def main():
    """CLI entry point for updating an endpoint."""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(
        description="Update a RunPod endpoint with a new template"
    )
    parser.add_argument(
        "--endpoint-id",
        required=True,
        help="ID of the endpoint to update"
    )
    parser.add_argument(
        "--template-id",
        required=True,
        help="New template ID to use"
    )
    parser.add_argument(
        "--name",
        help="New endpoint name (optional)"
    )
    parser.add_argument(
        "--gpu-ids",
        help="GPU type IDs (optional)"
    )
    parser.add_argument(
        "--workers-min",
        type=int,
        help="Minimum number of workers (optional)"
    )
    parser.add_argument(
        "--workers-max",
        type=int,
        help="Maximum number of workers (optional)"
    )
    
    args = parser.parse_args()
    
    try:
        result = update_endpoint(
            endpoint_id=args.endpoint_id,
            template_id=args.template_id,
            name=args.name,
            gpu_ids=args.gpu_ids,
            workers_min=args.workers_min,
            workers_max=args.workers_max
        )
        
        logger.info("✓ Endpoint updated successfully!")
        logger.info(f"Response: {result}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
