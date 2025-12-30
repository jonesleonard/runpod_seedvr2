"""
Find a RunPod endpoint by name using the REST API.

The RunPod SDK provides get_endpoints(), which we use to search by name.
"""

import os
import logging
from typing import Optional, Dict, Any
import runpod

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def find_endpoint_by_name(name: str, api_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Find an endpoint by name.
    
    Args:
        name: Endpoint name to search for
        api_key: RunPod API key (if not provided, uses RUNPOD_API_KEY env var)
    
    Returns:
        dict: Endpoint details if found, None otherwise
    """
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError("RUNPOD_API_KEY environment variable is required")
    
    runpod.api_key = api_key
    
    try:
        logger.info(f"Searching for endpoint with name: {name}")
        
        # Get all endpoints
        endpoints = runpod.get_endpoints()
        
        if not endpoints:
            logger.info("No endpoints found")
            return None
        
        # Search for endpoint by name
        for endpoint in endpoints:
            if endpoint.get("name") == name:
                logger.info(f"Found endpoint: {endpoint.get('id')}")
                return endpoint
        
        logger.info(f"No endpoint found with name: {name}")
        return None
    
    except Exception as e:
        logger.error(f"Error finding endpoint: {e}")
        raise


def main():
    """CLI entry point for finding an endpoint by name."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Find a RunPod endpoint by name"
    )
    parser.add_argument(
        "name",
        help="Endpoint name to search for"
    )
    
    args = parser.parse_args()
    
    try:
        endpoint = find_endpoint_by_name(args.name)
        
        if endpoint:
            logger.info(f"✓ Found endpoint: {endpoint.get('id')}")
            logger.info(f"Details: {endpoint}")
        else:
            logger.info(f"✗ Endpoint '{args.name}' not found")
    
    except Exception as e:
        logger.error(f"✗ Error: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
