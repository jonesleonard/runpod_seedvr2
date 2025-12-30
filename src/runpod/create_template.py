"""
Create a RunPod template for the SeedVR2 upscaler Docker image.

This script creates or updates a RunPod template with the appropriate
configuration for the video upscaling serverless function.
"""

import os
import sys
import argparse
import logging
from typing import Optional
import runpod
from find_template_by_id import template_exists

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_template(
    name: str,
    full_image_name: str,
    is_serverless: bool = True,
    env_vars: Optional[dict] = None,
    template_id: Optional[str] = None
) -> dict:
    """
    Create or update a RunPod template.
    
    Args:
        name: Template name 
        full_image_name: Docker image name (e.g., "repository/image-name:latest")
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
    if not full_image_name:
        full_image_name = os.environ.get("FULL_IMAGE_NAME")
        if not full_image_name:
            raise ValueError("FULL_IMAGE_NAME environment variable must be set")
    
    # Construct full image name
    
    logger.info(f"Creating/updating RunPod template for image: {full_image_name}")
    
    # Prepare template configuration
    template_config = {
        "name": name,
        "imageName": full_image_name,
        "dockerArgs": "",  # Add any docker arguments if needed
        "containerDiskInGb": 20,  # Adjust based on your needs
        "volumeInGb": 50,  # Storage for models and temporary files
        "volumeMountPath": "/work",
        "ports": "",  # Not needed for serverless
        "env": []
    }
    
    # Add environment variables if provided
    if env_vars:
        template_config["env"] = [
            {"key": k, "value": v} for k, v in env_vars.items()
        ]
    
    # Add serverless-specific configuration
    if is_serverless:
        template_config.update({
            "isServerless": True,
            "startSsh": False,
            "startJupyter": False,
        })
    
    try:
        # Initialize RunPod with API key
        runpod.api_key = api_key
        
        if template_id:
            # Check if template exists before updating
            if not template_exists(template_id, api_key):
                logger.warning(
                    f"Template ID {template_id} does not exist. "
                    "Creating new template instead."
                )
                template_id = None
        
        if template_id:
            # Update existing template
            logger.info(f"Updating template ID: {template_id}")
            response = runpod.update_template(template_id, **template_config)
        else:
            # Create new template
            logger.info("Creating new template")
            response = runpod.create_template(**template_config)
        
        logger.info("Template operation successful!")
        logger.info(f"Response: {response}")
        
        return response
    
    except Exception as e:
        logger.error(f"Failed to create/update template: {e}")
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
  FULL_IMAGE_NAME     - Full Docker image name (required if not specified with --image)
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
    
    # Parse environment variables
    env_vars = {}
    if args.env:
        for env_pair in args.env:
            try:
                key, value = env_pair.split("=", 1)
                # Check if template exists and handle --create-if-not-exists flag
                if args.create_if_not_exists and args.template_id:
                    api_key = os.environ.get("RUNPOD_API_KEY")
                    if template_exists(args.template_id, api_key):
                        logger.info(
                            f"Template {args.template_id} already exists. "
                            "Skipping creation (--create-if-not-exists flag set)."
                        )
                        sys.exit(0)
                
                        env_vars[key] = value
            except ValueError:
                logger.error(f"Invalid environment variable format: {env_pair}")
                sys.exit(1)
    
    try:
        result = create_template(
            name=args.name,
            full_image_name=args.full_image_name,
            env_vars=env_vars if env_vars else None,
            template_id=args.template_id
        )
        
        logger.info("✓ Template created/updated successfully!")
        
        if result and isinstance(result, dict):
            if "id" in result:
                logger.info(f"Template ID: {result['id']}")
            logger.info(f"Full response: {result}")
    
    except Exception as e:
        logger.error(f"✗ Failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
