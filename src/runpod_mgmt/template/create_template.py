"""
Create a RunPod template for the SeedVR2 upscaler Docker image.

This script creates or updates a RunPod template with the appropriate
configuration for the video upscaling serverless function.
"""

import os
import sys
import argparse
import logging
import traceback
from typing import Optional
import runpod
from .find_template_by_id import template_exists
from .update_template_by_id import update_template
from .find_template_by_name import find_template_by_name

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_template(
    name: str,
    image: str,
    container_disk_in_gb: int,
    volume_in_gb: int,
    is_serverless: bool = True,
    env_vars: Optional[dict] = None,
    template_id: Optional[str] = None
) -> dict:
    """
    Create or update a RunPod template.
    
    Args:
        name: Template name 
        image: Docker image name (e.g., "repository/image-name:latest")
        is_serverless: Whether this is a serverless template (default: True)
        env_vars: Optional environment variables for the template
        template_id: If provided, updates existing template instead of creating new
    
    Returns:
        dict: Response from RunPod API
    """
    # Get API key from environment
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        raise ValueError("RUNPOD_API_KEY environment variable is required")
    
    # Get full image name from environment if not provided
    if not image:
        image = os.environ.get("IMAGE")
        if not image:
            raise ValueError("IMAGE environment variable must be set")
        
    logger.info(f"Creating/updating RunPod template for image: {image}")
    
    # Prepare template configuration using SDK snake_case parameters
    template_config = {
        "name": name,
        "image_name": image,
        "container_disk_in_gb": container_disk_in_gb,  # Adjust based on your needs
        "volume_in_gb": volume_in_gb,  # Storage for models and temporary files
        "volume_mount_path": "/work",
        "is_serverless": is_serverless
    }
    
    # Add environment variables if provided
    if env_vars:
        template_config["env"] = env_vars
    
    try:
        # Initialize RunPod with API key
        runpod.api_key = api_key
        
        # Check if template_id is provided
        if template_id:
            # Check if template exists before updating
            if not template_exists(template_id, api_key):
                logger.warning(
                    f"Template ID {template_id} does not exist. "
                    "Creating new template instead."
                )
                template_id = None
        else:
            # No template_id provided, search by name
            existing_template = find_template_by_name(name, api_key)
            if existing_template:
                template_id = existing_template.get("id")
                logger.info(
                    f"Found existing template '{name}' with ID: {template_id}. "
                    "Will update it."
                )
        
        if template_id: 
            # Update existing template using REST API
            logger.info(f"Updating template ID: {template_id}")
            response = update_template(
                template_id=template_id,
                name=name,
                image_name=image,
                container_disk_in_gb=template_config["container_disk_in_gb"],
                volume_in_gb=template_config["volume_in_gb"],
                volume_mount_path=template_config["volume_mount_path"],
                env=template_config.get('env', None),
                api_key=api_key)
        else:
            # Create new template
            logger.info("Creating new template")
            response = runpod.create_template(**template_config)
        
        logger.info("Template operation successful!")
        logger.info(f"Response: {response}")
        
        return response
    
    except Exception as e:
        logger.error(f"Failed to create/update template: {e}")
        logger.error("".join(traceback.format_exc()))
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Create or update a RunPod template for SeedVR2 upscaler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a new template with latest tag:
  python create_template.py --name "SeedVR2 Upscaler" --image seedvr2-upscaler
  
  # Create with specific Docker tag:
  python create_template.py --name "SeedVR2 Upscaler" --image seedvr2-upscaler --tag v1.0.0
  
  # Update an existing template:
  python create_template.py --name "SeedVR2 Upscaler" --image seedvr2-upscaler --template-id YOUR_TEMPLATE_ID
  
Environment Variables:
  RUNPOD_API_KEY      - Your RunPod API key (required)
  IMAGE     - Full Docker image name (required if not specified with --image)
        """
    )
    
    parser.add_argument(
        "--name",
        default="SeedVR2 Video Upscaler",
        help="Template name (default: 'SeedVR2 Video Upscaler')"
    )
    
    parser.add_argument(
        "--image",
        default="seedvr2-upscaler",
        help="Docker image name (default: 'seedvr2-upscaler')"
    )

    parser.add_argument(
        "--template-id",
        help="Existing template ID to update (creates new if not specified)"
    )
    
    parser.add_argument(
        "--create-if-not-exists",
        action="store_true",
        help="Only create template if it doesn't exist (skip update)"
    )

    serverless_group = parser.add_mutually_exclusive_group()
    serverless_group.add_argument(
        "--serverless",
        dest="is_serverless",
        action="store_true",
        help="Create a serverless template (default)"
    )
    serverless_group.add_argument(
        "--non-serverless",
        dest="is_serverless",
        action="store_false",
        help="Create a non-serverless template"
    )
    parser.set_defaults(is_serverless=True)
    
    parser.add_argument(
        "--env",
        action="append",
        metavar="KEY=VALUE",
        help="Environment variables to set (can be specified multiple times)"
    )
    
    parser.add_argument(
        "--container-disk",
        type=int,
        default=20,
        help="Container disk size in GB (default: 20)"
    )
    
    parser.add_argument(
        "--volume",
        type=int,
        default=50,
        help="Volume size in GB (default: 50)"
    )
    
    args = parser.parse_args()
    
    # Check if template exists and handle --create-if-not-exists flag
    if args.create_if_not_exists and args.template_id:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if template_exists(args.template_id, api_key):
            logger.info(
                f"Template {args.template_id} already exists. "
                "Skipping creation (--create-if-not-exists flag set)."
            )
            sys.exit(0)
    
    # Parse environment variables
    env_vars = {}
    if args.env:
        logger.info("Setting environment variables:")
        for env_pair in args.env:
            logger.info(f"  {env_pair}")
            try:
                key, value = env_pair.split("=", 1)
                env_vars[key] = value
                logger.info(f"  Parsed: {key}={value}")
            except ValueError:
                logger.error(f"Invalid environment variable format: {env_pair}")
                sys.exit(1)
    
    try:
        result = create_template(
            name=args.name,
            image=args.image,
            is_serverless=args.is_serverless,
            env_vars=env_vars if env_vars else None,
            template_id=args.template_id
        )
        
        logger.info("✓ Template created/updated successfully!")
        
        if result and isinstance(result, dict):
            if "id" in result:
                template_id = result['id']
                logger.info(f"Template ID: {template_id}")
                with open(os.environ.get("GITHUB_OUTPUT", "/dev/stdout"), "a") as gh_out:
                    gh_out.write(f"template_id={result['id']}\n")
            else:
                logger.error(f"Template ID not found in response: {result}")
                raise ValueError("Template ID missing in response - cannot set output.")
        else:
            logger.info(f"Full response: {result}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
