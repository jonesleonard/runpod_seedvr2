"""
Find a RunPod network volume by name using the REST API.

The RunPod SDK doesn't provide a method to search network volumes by name,
so this script uses the REST API directly.
"""

import os
import logging
from typing import Optional, Dict, Any, List
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RUNPOD_REST_API_BASE_URL = os.environ.get("RUNPOD_REST_API_BASE_URL")


def list_network_volumes(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all network volumes for the account.
    
    Args:
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        list: List of network volumes
    """
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError("RUNPOD_API_KEY must be set")
    
    url = f"{RUNPOD_REST_API_BASE_URL}/networkvolumes"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        return data if isinstance(data, list) else []
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to list network volumes: {e}")
        return []


def find_network_volume_by_name(
    name: str,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a RunPod network volume by its name.
    
    Args:
        name: The network volume name to search for
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        dict: Network volume information if found, None if not found
    """
    network_volumes = list_network_volumes(api_key)
    
    for network_volume in network_volumes:
        if network_volume.get("name") == name:
            logger.info(f"Found network volume '{name}' with ID: {network_volume.get('id')}")
            return network_volume
    
    logger.info(f"Network volume not found: {name}")
    return None


if __name__ == "__main__":
    import sys
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Find a RunPod network volume by name")
    parser.add_argument("name", help="Network volume name to search for")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    try:
        network_volume = find_network_volume_by_name(args.name)
        
        if network_volume:
            if args.json:
                print(json.dumps(network_volume, indent=2))
            else:
                print(f"Found: {network_volume.get('name')} (ID: {network_volume.get('id')})")
            sys.exit(0)
        else:
            print(f"Network volume '{args.name}' not found")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
