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

RUNPOD_API_BASE = "https://rest.runpod.io/v1"


def find_template_by_id(
    template_id: str,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a RunPod template by its ID.
    
    Args:
        template_id: The template ID to search for
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        dict: Template information if found, None if not found
    
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
    headers = {"Authorization": f"Bearer {api_key}"}
    
    logger.info(f"Checking if template exists: {template_id}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        # 404 means template doesn't exist
        if response.status_code == 404:
            logger.info(f"Template not found: {template_id}")
            return None
        
        # Raise for other error status codes
        response.raise_for_status()
        
        template = response.json()
        logger.info(f"Template found: {template.get('name')} (ID: {template_id})")
        return template
    
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        logger.error(f"HTTP error occurred: {e}")
        raise
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch template: {e}")
        raise


def template_exists(
    template_id: str,
    api_key: Optional[str] = None
) -> bool:
    """
    Check if a template exists by ID.
    
    Args:
        template_id: The template ID to check
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        bool: True if template exists, False otherwise
    """
    try:
        template = find_template_by_id(template_id, api_key)
        return template is not None
    except Exception as e:
        logger.error(f"Error checking template existence: {e}")
        return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Find a RunPod template by ID",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check if a template exists:
  python find_template_by_id.py 30zmvf89kd
  
  # Get template details as JSON:
  python find_template_by_id.py 30zmvf89kd --json
  
  # Check existence (returns exit code 0 if exists, 1 if not):
  python find_template_by_id.py 30zmvf89kd --exists-only
  
Environment Variables:
  RUNPOD_API_KEY - Your RunPod API key (required)
        """
    )
    
    parser.add_argument(
        "template_id",
        help="Template ID to search for"
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
        template = find_template_by_id(args.template_id)
        
        if args.exists_only:
            # Exit with appropriate code
            sys.exit(0 if template else 1)
        
        if template:
            if args.json:
                print(json.dumps(template, indent=2))
            else:
                logger.info("Template details:")
                logger.info(f"  Name: {template.get('name')}")
                logger.info(f"  ID: {template.get('id')}")
                logger.info(f"  Image: {template.get('imageName')}")
                logger.info(
                    f"  Serverless: {template.get('isServerless', False)}"
                )
                logger.info(f"  Public: {template.get('isPublic', False)}")
                logger.info(
                    f"  Container Disk: {template.get('containerDiskInGb')}GB"
                )
                logger.info(f"  Volume: {template.get('volumeInGb')}GB")
            sys.exit(0)
        else:
            logger.warning("Template not found")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
