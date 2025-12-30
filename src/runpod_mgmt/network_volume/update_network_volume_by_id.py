"""
Update a RunPod network volume using the REST API.

This script patches a network volume's properties (e.g., `name`, `size`) via
RunPod's REST API endpoint `/networkvolumes/{networkVolumeId}`. It mirrors the
structure and ergonomics of our template update script for consistency.
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

RUNPOD_REST_API_BASE_URL = os.environ.get("RUNPOD_REST_API_BASE_URL", "https://rest.runpod.io/v1")

def update_network_volume(
    network_volume_id: str,
    name: Optional[str] = None,
    size: Optional[int] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Update a RunPod network volume using the REST API.

    Parameters
    ----------
    network_volume_id : str
        The ID of the network volume to update.
    name : Optional[str]
        New name for the network volume.
    size : Optional[int]
        New size for the network volume (in GB).
    api_key : Optional[str]
        RunPod API key. If not provided, uses RUNPOD_API_KEY environment variable.

    Returns
    -------
    Dict[str, Any]
        Parsed JSON response from RunPod.

    Raises
    ------
    ValueError
        If API key is missing or no update fields are provided.
    requests.exceptions.RequestException
        If the API request fails.
    """
    # Resolve API key
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError(
                "API key must be provided or RUNPOD_API_KEY environment variable must be set"
            )

    # Validate inputs
    if name is None and size is None:
        raise ValueError("At least one of --name or --size must be provided")
    if size is not None and size <= 0:
        raise ValueError("--size must be a positive integer (GB)")

    url = f"{RUNPOD_REST_API_BASE_URL}/networkvolumes/{network_volume_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Build payload (only provided fields)
    payload: Dict[str, Any] = {}
    if name is not None:
        payload["name"] = name
    if size is not None:
        payload["size"] = size

    logger.info(f"Updating network volume {network_volume_id}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.patch(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        logger.info("Network volume updated successfully")
        return result
    except requests.exceptions.HTTPError as e:
        # Include server-provided error text if available
        err_text = getattr(e.response, "text", "")
        logger.error(f"HTTP error updating network volume: {e}")
        if err_text:
            logger.error(f"Response: {err_text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update network volume: {e}")
        raise


def main():
    """CLI entry point for updating a network volume."""
    parser = argparse.ArgumentParser(
        description="Update a RunPod network volume using the REST API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update network volume name:
  python update_network_volume_by_id.py YOUR_VOLUME_ID --name "My Network Volume"

  # Update network volume size (GB):
  python update_network_volume_by_id.py YOUR_VOLUME_ID --size 100

Environment Variables:
  RUNPOD_API_KEY - Your RunPod API key (required)
  RUNPOD_REST_API_BASE_URL - Optional; default: https://rest.runpod.io/v1
        """
    )

    parser.add_argument(
        "network_volume_id",
        help="Network volume ID to update"
    )

    parser.add_argument(
        "--name",
        help="New name for the network volume"
    )

    parser.add_argument(
        "--size",
        type=int,
        help="New size for the network volume (in GB)"
    )

    parser.add_argument(
        "--json",
        action="store_true",
        help="Output response as JSON"
    )

    args = parser.parse_args()

    try:
        result = update_network_volume(
            network_volume_id=args.network_volume_id,
            name=args.name,
            size=args.size,
        )

        if args.json:
            print(json.dumps(result, indent=2))
        else:
            logger.info("✓ Network volume updated successfully!")
            if isinstance(result, dict):
                logger.info(f"ID: {result.get('id', args.network_volume_id)}")
                logger.info(f"Name: {result.get('name')}")
                logger.info(f"Size (GB): {result.get('size')}")
                dc = result.get('dataCenterId')
                if dc:
                    logger.info(f"Data Center: {dc}")

    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
