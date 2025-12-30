"""
Find a RunPod template by name using the REST API.

The RunPod SDK doesn't provide a method to search templates by name,
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


def list_templates(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all templates for the account.
    
    Args:
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        list: List of templates
    """
    if not api_key:
        api_key = os.environ.get("RUNPOD_API_KEY")
        if not api_key:
            raise ValueError("RUNPOD_API_KEY must be set")
    
    url = f"{RUNPOD_REST_API_BASE_URL}/templates"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # API might return {"templates": [...]} or just [...]
        data = response.json()
        if isinstance(data, dict) and "templates" in data:
            return data["templates"]
        return data if isinstance(data, list) else []
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to list templates: {e}")
        return []


def find_template_by_name(
    name: str,
    api_key: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Find a RunPod template by its name.
    
    Args:
        name: The template name to search for
        api_key: RunPod API key (uses RUNPOD_API_KEY env var if not provided)
    
    Returns:
        dict: Template information if found, None if not found
    """
    templates = list_templates(api_key)
    
    for template in templates:
        if template.get("name") == name:
            logger.info(f"Found template '{name}' with ID: {template.get('id')}")
            return template
    
    logger.info(f"Template not found: {name}")
    return None


if __name__ == "__main__":
    import sys
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description="Find a RunPod template by name")
    parser.add_argument("name", help="Template name to search for")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    try:
        template = find_template_by_name(args.name)
        
        if template:
            if args.json:
                print(json.dumps(template, indent=2))
            else:
                print(f"Found: {template.get('name')} (ID: {template.get('id')})")
                print(f"Image: {template.get('imageName')}")
            sys.exit(0)
        else:
            print(f"Template '{args.name}' not found")
            sys.exit(1)
    
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
