"""
Create a RunPod pod using the SDK.

This script creates a pod with a template ID (preferred) or image name,
optionally attaching a network volume and passing environment variables.
"""

import os
import sys
import argparse
import logging
import traceback
from typing import Optional, Dict, Any
import runpod

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_pod(
    name: str,
    template_id: Optional[str] = None,
    image_name: Optional[str] = None,
    gpu_type_id: Optional[str] = None,
    gpu_count: int = 1,
    cloud_type: str = "ALL",
    data_center_id: Optional[str] = None,
    network_volume_id: Optional[str] = None,
    volume_mount_path: str = "/runpod-volume",
    ports: Optional[str] = None,
    env_vars: Optional[Dict[str, str]] = None,
    start_ssh: bool = True,
    support_public_ip: bool = True,
) -> Dict[str, Any]:
    """
    Create a RunPod pod.

    Args:
        name: Pod name.
        template_id: Template ID to use (preferred for template-based pods).
        image_name: Docker image name (used if template_id is not provided).
        gpu_type_id: GPU type ID (optional for CPU pods).
        gpu_count: Number of GPUs to attach (ignored for CPU pods).
        cloud_type: Cloud type (ALL, COMMUNITY, SECURE).
        data_center_id: Data center ID (optional).
        network_volume_id: Network volume ID to attach (optional).
        volume_mount_path: Mount path for the network volume.
        ports: Port mappings (e.g., "22/tcp,8888/http").
        env_vars: Environment variables to inject into the pod.
        start_ssh: Whether to start SSH in the pod.
        support_public_ip: Whether to request a public IP.
    """
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not api_key:
        raise ValueError("RUNPOD_API_KEY environment variable is required")

    if not template_id and not image_name:
        raise ValueError("Either template_id or image_name must be provided")

    runpod.api_key = api_key

    response = runpod.create_pod(
        name=name,
        image_name=image_name or "",
        gpu_type_id=gpu_type_id,
        gpu_count=gpu_count,
        cloud_type=cloud_type,
        data_center_id=data_center_id,
        network_volume_id=network_volume_id,
        volume_mount_path=volume_mount_path,
        ports=ports,
        env=env_vars,
        template_id=template_id,
        start_ssh=start_ssh,
        support_public_ip=support_public_ip,
    )

    return response


def _parse_env_vars(env_list) -> Optional[Dict[str, str]]:
    if not env_list:
        return None

    env_vars: Dict[str, str] = {}
    for env_pair in env_list:
        try:
            key, value = env_pair.split("=", 1)
            env_vars[key] = value
        except ValueError:
            raise ValueError(f"Invalid environment variable format: {env_pair}") from None

    return env_vars


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a RunPod pod using the SDK",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a pod from a template:
  python create_pod.py --name "model-download" --template-id TEMPLATE_ID

  # Create a pod with a network volume:
  python create_pod.py --name "model-download" --template-id TEMPLATE_ID \\
    --network-volume-id NETWORK_VOLUME_ID --volume-mount-path /runpod-volume

Environment Variables:
  RUNPOD_API_KEY - Your RunPod API key (required)
        """,
    )

    parser.add_argument("--name", required=True, help="Pod name")
    parser.add_argument("--template-id", help="Template ID to use")
    parser.add_argument("--image-name", help="Docker image name (if no template is used)")
    parser.add_argument("--gpu-type-id", help="GPU type ID (optional)")
    parser.add_argument("--gpu-count", type=int, default=1, help="Number of GPUs (default: 1)")
    parser.add_argument(
        "--cloud-type",
        default="ALL",
        choices=["ALL", "COMMUNITY", "SECURE"],
        help="Cloud type (default: ALL)",
    )
    parser.add_argument("--data-center-id", help="Data center ID")
    parser.add_argument("--network-volume-id", help="Network volume ID to attach")
    parser.add_argument(
        "--volume-mount-path",
        default="/runpod-volume",
        help="Network volume mount path (default: /runpod-volume)",
    )
    parser.add_argument("--ports", help="Ports to expose (e.g., '22/tcp,8888/http')")
    parser.add_argument(
        "--env",
        action="append",
        metavar="KEY=VALUE",
        help="Environment variables (can be specified multiple times)",
    )
    parser.add_argument(
        "--start-ssh",
        dest="start_ssh",
        action="store_true",
        help="Start SSH in the pod (default)",
    )
    parser.add_argument(
        "--no-start-ssh",
        dest="start_ssh",
        action="store_false",
        help="Do not start SSH in the pod",
    )
    parser.add_argument(
        "--public-ip",
        dest="support_public_ip",
        action="store_true",
        help="Request a public IP (default)",
    )
    parser.add_argument(
        "--no-public-ip",
        dest="support_public_ip",
        action="store_false",
        help="Do not request a public IP",
    )
    parser.add_argument("--json", action="store_true", help="Output response as JSON")
    parser.set_defaults(start_ssh=True, support_public_ip=True)

    args = parser.parse_args()

    try:
        env_vars = _parse_env_vars(args.env)

        result = create_pod(
            name=args.name,
            template_id=args.template_id,
            image_name=args.image_name,
            gpu_type_id=args.gpu_type_id,
            gpu_count=args.gpu_count,
            cloud_type=args.cloud_type,
            data_center_id=args.data_center_id,
            network_volume_id=args.network_volume_id,
            volume_mount_path=args.volume_mount_path,
            ports=args.ports,
            env_vars=env_vars,
            start_ssh=args.start_ssh,
            support_public_ip=args.support_public_ip,
        )

        if args.json:
            import json

            print(json.dumps(result, indent=2))
        else:
            logger.info("✓ Pod created successfully!")
            if isinstance(result, dict):
                logger.info(f"Pod ID: {result.get('id')}")
                logger.info(f"Image: {result.get('imageName')}")

        if isinstance(result, dict) and "id" in result:
            with open(os.environ.get("GITHUB_OUTPUT", "/dev/stdout"), "a") as gh_out:
                gh_out.write(f"pod_id={result['id']}\n")

    except Exception as exc:
        logger.error(f"✗ Failed: {exc}")
        logger.error("".join(traceback.format_exc()))
        sys.exit(1)


if __name__ == "__main__":
    main()
