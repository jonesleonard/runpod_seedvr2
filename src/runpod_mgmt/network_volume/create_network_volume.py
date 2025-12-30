"""
Create a RunPod network volume for the SeedVR2 upscaler models.

This script creates or updates a RunPod network volume to store
the upscaler models persistently.
"""

import os
import sys
import argparse
import logging
import traceback
from typing import Optional
import requests
from .find_network_volume_by_id import network_volume_exists
from .update_network_volume_by_id import update_network_volume
from .find_network_volume_by_name import find_network_volume_by_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RUNPOD_REST_API_BASE_URL = os.environ.get("RUNPOD_REST_API_BASE_URL", "https://rest.runpod.io/v1")


def create_network_volume(
    name: str,
    data_center_id: str,
    size: int = 50,
    network_volume_id: Optional[str] = None
) -> dict:
    """
    Create or update a RunPod network volume.
    
    Args:
        name: Network volume name 
        data_center_id: Data center ID where the network volume will be created
        size: Size of the network volume in GB (default: 50)
        network_volume_id: If provided, updates existing network volume instead of creating new
    
    Returns:
        dict: Response from RunPod API
    """
    # Get API key from environment
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        raise ValueError("RUNPOD_API_KEY environment variable is required")
    
    if size <= 0:
        raise ValueError("size must be a positive integer (GB)")

    # Prepare network volume configuration
    network_volume_config = {
        "name": name,
        "dataCenterId": data_center_id,
        "size": size
    }
    
    
    try:
        # Check if network_volume_id is provided
        if network_volume_id:
            # Check if network volume exists before updating
            if not network_volume_exists(network_volume_id, api_key):
                logger.warning(
                    f"Network volume ID {network_volume_id} does not exist. "
                    "Creating new network volume instead."
                )
                network_volume_id = None
        else:
            # No network_volume_id provided, search by name
            existing_network_volume = find_network_volume_by_name(name, api_key)
            if existing_network_volume:
                network_volume_id = existing_network_volume.get("id")
                logger.info(
                    f"Found existing network volume '{name}' with ID: {network_volume_id}. "
                    "Will update it."
                )
        
        if network_volume_id: 
            # Update existing network volume using REST API
            logger.info(f"Updating network volume ID: {network_volume_id}")
            response = update_network_volume(
                network_volume_id=network_volume_id,
                name=name,
                size=size,
                api_key=api_key,
            )
        else:
            # Create new network volume
            logger.info("Creating new network volume")
            url = f"{RUNPOD_REST_API_BASE_URL}/networkvolumes"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            response = requests.post(
                url,
                json=network_volume_config,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            response = response.json()
        
        logger.info("Network volume operation successful!")
        logger.info(f"Response: {response}")
        
        return response
    
    except requests.exceptions.HTTPError as e:
        err_text = getattr(e.response, "text", "")
        logger.error(f"HTTP error creating/updating network volume: {e}")
        if err_text:
            logger.error(f"Response: {err_text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create/update network volume: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to create/update network volume: {e}")
        logger.error("".join(traceback.format_exc()))
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Create or update a RunPod network volume for SeedVR2 upscaler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a new network volume:
  python create_network_volume.py --name "network_volume" --data-center-id "EU-RO-1" --size 50
  
  # Update an existing network volume:
  python create_network_volume.py --name "network_volume" --data-center-id "EU-RO-1" --size 50 --network-volume-id YOUR_NETWORK_VOLUME_ID
  
Environment Variables:
  RUNPOD_API_KEY      - Your RunPod API key (required)
        """
    )
    
    parser.add_argument(
        "--name",
        default="SeedVR2 Models Volume",
        help="Network volume name (default: 'SeedVR2 Models Volume')"
    )
    
    parser.add_argument(
        "--data-center-id",
        default="EU-RO-1",
        help="Data center ID (default: 'EU-RO-1')"
    )

    parser.add_argument(
        "--network-volume-id",
        help="Existing network volume ID to update (creates new if not specified)"
    )
    
    parser.add_argument(
        "--create-if-not-exists",
        action="store_true",
        help="Only create network volume if it doesn't exist (skip update)"
    )
    
    parser.add_argument(
        "--size",
        type=int,
        default=50,
        help="Network volume size in GB (default: 50)"
    )
    
    args = parser.parse_args()
    
    # Check if network volume exists and handle --create-if-not-exists flag
    if args.create_if_not_exists and args.network_volume_id:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if network_volume_exists(args.network_volume_id, api_key):
            logger.info(
                f"Network volume {args.network_volume_id} already exists. "
                "Skipping creation (--create-if-not-exists flag set)."
            )
            sys.exit(0)
    
    try:
        result = create_network_volume(
            name=args.name,
            data_center_id=args.data_center_id,
            size=args.size,
            network_volume_id=args.network_volume_id
        )
        
        logger.info("✓ Network volume created/updated successfully!")
        
        if result and isinstance(result, dict):
            if "id" in result:
                network_volume_id = result['id']
                logger.info(f"Network Volume ID: {network_volume_id}")
                with open(os.environ.get("GITHUB_OUTPUT", "/dev/stdout"), "a") as gh_out:
                    gh_out.write(f"network_volume_id={result['id']}\n")
            else:
                logger.error(f"Network Volume ID not found in response: {result}")
                raise ValueError("Network Volume ID missing in response - cannot set output.")
        else:
            logger.info(f"Full response: {result}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
