"""
Create or update a RunPod serverless endpoint.

This script creates a new endpoint or updates an existing one to use a new template.
"""

import os
import sys
import argparse
import logging
from typing import Optional
import runpod
from .find_endpoint_by_name import find_endpoint_by_name
from .update_endpoint import update_endpoint

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_or_update_endpoint(
    name: str,
    template_id: str,
    gpu_ids: str = "AMPERE_16",
    workers_min: int = 0,
    workers_max: int = 1,
    idle_timeout: int = 5,
    execution_timeout_ms: int = 600000,
    scaler_type: str = "QUEUE_DELAY",
    scaler_value: int = 4,
    endpoint_id: Optional[str] = None
) -> dict:
    """
    Create a new endpoint or update an existing one.
    
    Args:
        name: Endpoint name
        template_id: Template ID to use
        gpu_ids: GPU type IDs (default: "AMPERE_16")
        workers_min: Minimum number of workers (default: 0)
        workers_max: Maximum number of workers (default: 1)
        idle_timeout: Idle timeout in seconds (default: 5)
        execution_timeout_ms: Execution timeout in milliseconds (default: 600000)
        scaler_type: Scaler type (default: "QUEUE_DELAY")
        scaler_value: Scaler value (default: 4)
        endpoint_id: If provided, updates existing endpoint instead of creating new
    
    Returns:
        dict: Response from RunPod API
    """
    # Get API key from environment
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        raise ValueError("RUNPOD_API_KEY environment variable is required")
    
    runpod.api_key = api_key
    
    # Check if we should update an existing endpoint
    if endpoint_id:
        logger.info(f"Updating endpoint ID: {endpoint_id}")
        return update_endpoint(
            endpoint_id=endpoint_id,
            template_id=template_id,
            api_key=api_key,
            name=name,
            gpu_ids=gpu_ids,
            workers_min=workers_min,
            workers_max=workers_max,
            idle_timeout=idle_timeout,
            execution_timeout_ms=execution_timeout_ms,
            scaler_type=scaler_type,
            scaler_value=scaler_value
        )
    
    # Search for existing endpoint by name
    existing_endpoint = find_endpoint_by_name(name, api_key)
    
    if existing_endpoint:
        endpoint_id = existing_endpoint.get("id")
        logger.info(f"Found existing endpoint '{name}' with ID: {endpoint_id}")
        logger.info("Updating endpoint with new template...")
        
        return update_endpoint(
            endpoint_id=endpoint_id,
            template_id=template_id,
            api_key=api_key,
            name=name,
            gpu_ids=gpu_ids,
            workers_min=workers_min,
            workers_max=workers_max,
            idle_timeout=idle_timeout,
            execution_timeout_ms=execution_timeout_ms,
            scaler_type=scaler_type,
            scaler_value=scaler_value
        )
    
    # Create new endpoint
    logger.info(f"Creating new endpoint: {name}")
    
    try:
        response = runpod.create_endpoint(
            name=name,
            template_id=template_id,
            gpu_ids=gpu_ids,
            workers_min=workers_min,
            workers_max=workers_max,
            idle_timeout=idle_timeout,
            execution_timeout_ms=execution_timeout_ms,
            scaler_type=scaler_type,
            scaler_value=scaler_value
        )
        
        logger.info("Endpoint created successfully!")
        return response
    
    except Exception as e:
        logger.error(f"Failed to create endpoint: {e}")
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Create or update a RunPod serverless endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a new endpoint:
  python create_endpoint.py --name "My Endpoint" --template-id YOUR_TEMPLATE_ID
  
  # Update an existing endpoint:
  python create_endpoint.py --name "My Endpoint" --template-id NEW_TEMPLATE_ID --endpoint-id YOUR_ENDPOINT_ID
  
  # Create with custom settings:
  python create_endpoint.py --name "My Endpoint" --template-id YOUR_TEMPLATE_ID --gpu-ids "AMPERE_16" --workers-max 3

Environment Variables:
  RUNPOD_API_KEY              - Your RunPod API key (required)
  RUNPOD_REST_API_BASE_URL    - RunPod REST API base URL (default: https://rest.runpod.io/v1)
        """
    )
    
    parser.add_argument(
        "--name",
        required=True,
        help="Endpoint name"
    )
    
    parser.add_argument(
        "--template-id",
        required=True,
        help="Template ID to use for the endpoint"
    )
    
    parser.add_argument(
        "--endpoint-id",
        help="Existing endpoint ID to update (creates new if not specified)"
    )
    
    parser.add_argument(
        "--gpu-ids",
        default="AMPERE_16",
        help="GPU type IDs (default: AMPERE_16)"
    )
    
    parser.add_argument(
        "--workers-min",
        type=int,
        default=0,
        help="Minimum number of workers (default: 0)"
    )
    
    parser.add_argument(
        "--workers-max",
        type=int,
        default=1,
        help="Maximum number of workers (default: 1)"
    )
    
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=5,
        help="Idle timeout in seconds (default: 5)"
    )
    
    parser.add_argument(
        "--execution-timeout",
        type=int,
        default=600000,
        help="Execution timeout in milliseconds (default: 600000)"
    )
    
    parser.add_argument(
        "--scaler-type",
        default="QUEUE_DELAY",
        help="Scaler type (default: QUEUE_DELAY)"
    )
    
    parser.add_argument(
        "--scaler-value",
        type=int,
        default=4,
        help="Scaler value (default: 4)"
    )
    
    args = parser.parse_args()
    
    try:
        result = create_or_update_endpoint(
            name=args.name,
            template_id=args.template_id,
            gpu_ids=args.gpu_ids,
            workers_min=args.workers_min,
            workers_max=args.workers_max,
            idle_timeout=args.idle_timeout,
            execution_timeout_ms=args.execution_timeout,
            scaler_type=args.scaler_type,
            scaler_value=args.scaler_value,
            endpoint_id=args.endpoint_id
        )
        
        logger.info("✓ Endpoint created/updated successfully!")
        
        if result and isinstance(result, dict):
            if "id" in result:
                logger.info(f"Endpoint ID: {result['id']}")
                with open(os.environ.get("GITHUB_OUTPUT", "/dev/stdout"), "a") as gh_out:
                    gh_out.write(f"endpoint_id={result['id']}\n")
            else:
                logger.error(f"Template ID not found in response: {result}")
                raise ValueError("Template ID missing in response - cannot set output.")

            logger.info(f"Full response: {result}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()