"""
Find a RunPod template by ID using the REST API.

The RunPod SDK doesn't provide a method to get a template by ID,
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

RUNPOD_REST_API_BASE_URL = os.environ.get("RUNPOD_REST_API_BASE_URL")

def find_network_volume_by_id(
    network_volume_id: str,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a RunPod network volume by its ID.
    
    Args:
        network_volume_id: The network volume ID to search for
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        dict: Network volume information if found, None if not found
    
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
    
    url = f"{RUNPOD_REST_API_BASE_URL}/networkvolumes/{network_volume_id}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    logger.info(f"Checking if network volume exists: {network_volume_id}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        # 404 means network volume doesn't exist
        if response.status_code == 404:
            logger.info(f"Network volume not found: {network_volume_id}")
            return None
        
        # Raise for other error status codes
        response.raise_for_status()
        
        network_volume = response.json()
        logger.info(f"Network volume found: {network_volume.get('name')} (ID: {network_volume_id})")
        return network_volume
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        logger.error(f"HTTP error occurred: {e}")
        raise
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch template: {e}")
        raise


def network_volume_exists(
    network_volume_id: str,
    api_key: Optional[str] = None
) -> bool:
    """
    Check if a network volume exists by ID.
    
    Args:
        network_volume_id: The network volume ID to check
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        bool: True if network volume exists, False otherwise
    """
    try:
        network_volume = find_network_volume_by_id(network_volume_id, api_key)
        return network_volume is not None
    except Exception as e:
        logger.error(f"Error checking network volume existence: {e}")
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Find a RunPod network volume by ID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check if a network volume exists:
  python find_network_volume_by_id.py 30zmvf89kd
  
  # Get network volume details as JSON:
  python find_network_volume_by_id.py 30zmvf89kd --json
  
  # Check existence (returns exit code 0 if exists, 1 if not):
  python find_network_volume_by_id.py 30zmvf89kd --exists-only
  
Environment Variables:
  RUNPOD_API_KEY - Your RunPod API key (required)
        """
    )
    
    parser.add_argument(
        "network_volume_id",
        help="Network volume ID to search for"
    )
    
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output template details as JSON"
    )
    
    parser.add_argument(
        "--exists-only",
        action="store_true",
        help="Only check if template exists (exit code 0=exists, 1=not found)"
    )
    
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational output"
    )
    
    args = parser.parse_args()
    
    if args.quiet:
        logger.setLevel(logging.ERROR)
    
    try:
        network_volume = find_network_volume_by_id(args.network_volume_id)
        
        if args.exists_only:
            # Exit with appropriate code
            sys.exit(0 if network_volume else 1)
        
        if network_volume:
            if args.json:
                print(json.dumps(network_volume, indent=2))
            else:
                logger.info("Network volume details:")
                logger.info(f"  Name: {network_volume.get('name')}")
                logger.info(f"  ID: {network_volume.get('id')}")
                logger.info(f"  Image: {network_volume.get('imageName')}")
                logger.info(
                    f"  Serverless: {network_volume.get('isServerless', False)}"
                )
                logger.info(f"  Public: {network_volume.get('isPublic', False)}")
                logger.info(
                    f"  Container Disk: {network_volume.get('containerDiskInGb')}GB"
                )
                logger.info(f"  Volume: {network_volume.get('volumeInGb')}GB")
            sys.exit(0)
        else:
            logger.warning("Network volume not found")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
