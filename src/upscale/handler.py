"""
RunPod serverless handler for video upscaling using SeedVR2.
Delegates to upscale_segment.sh for the actual work.
"""

import os
import subprocess
import logging
import time
import json
from pathlib import Path
from typing import Dict, Any, List
from urllib.parse import urlparse
from urllib.request import urlopen
import runpod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRIPT_PATH = "/app/upscale_segment.sh"
MODELS_DEFAULT_DIR = "/models"
_MODELS_READY = False


def _get_model_urls(job_input: Dict[str, Any]) -> List[str]:
    urls = []
    for key in ("vae_model_presigned_url", "dit_model_presigned_url"):
        value = job_input.get(key)
        if value:
            urls.append(str(value).strip())

    return [u for u in urls if u]


def _filename_from_url(url: str) -> str:
    filename = Path(urlparse(url).path).name
    if not filename:
        raise ValueError(f"Could not infer filename from URL: {url}")
    return filename


def _download_file(url: str, dest_path: Path) -> None:
    logger.info("Downloading %s -> %s", url, dest_path)
    with urlopen(url, timeout=60) as response, open(dest_path, "wb") as out_file:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out_file.write(chunk)
    logger.info("Downloaded %s (%s bytes)", dest_path, dest_path.stat().st_size)


def _ensure_models_downloaded(job_input: Dict[str, Any]) -> None:
    global _MODELS_READY
    if _MODELS_READY:
        return

    urls = _get_model_urls(job_input)
    if not urls:
        logger.info("No model URLs provided; skipping model download.")
        _MODELS_READY = True
        return

    models_dir = Path(os.environ.get("MODELS_DIR", MODELS_DEFAULT_DIR))
    models_dir.mkdir(parents=True, exist_ok=True)

    for url in urls:
        filename = _filename_from_url(url)
        dest_path = models_dir / filename
        if dest_path.exists() and dest_path.stat().st_size > 0:
            logger.info("Model already present: %s", dest_path)
            continue
        _download_file(url, dest_path)

    _MODELS_READY = True


def upscale_segment(job: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upscale a single video segment by invoking the shell script.
    
    Expected job_input:
    {
        "input_presigned_url": "https://bucket.s3.../segment.mp4?X-Amz-...",
        "output_presigned_url": "https://bucket.s3.../output.mp4?X-Amz-...",
        "params": {
            "model": "7b",
            "resolution": 1080,
            "seed": 42,
            ...
        }
    }
    
    Models are loaded from local storage (default: /models).
    """
    start_time = time.time()
    
    try:

        # debug log full job
        logger.debug(f"Received job: {json.dumps(job, indent=2)}")

        # Support both "input" wrapper and direct job dict
        job_input = job.get("input", job)

        # Validate input - now using presigned URLs instead of S3 URIs
        input_presigned_url = job_input.get("input_presigned_url")
        output_presigned_url = job_input.get("output_presigned_url")
        vae_model_presigned_url = job_input.get("vae_model_presigned_url")
        dit_model_presigned_url = job_input.get("dit_model_presigned_url")
        params = job_input.get("params", {})
        
        if not input_presigned_url or not output_presigned_url:
            raise ValueError("input_presigned_url and output_presigned_url are required")
        if not vae_model_presigned_url or not dit_model_presigned_url:
            raise ValueError(
                "vae_model_presigned_url and dit_model_presigned_url are required"
            )

        _ensure_models_downloaded(job_input)
        
        logger.info("Starting upscale job with presigned URLs")
        
        # Build environment variables for the shell script
        env = os.environ.copy()
        env.update({
            "INPUT_PRESIGNED_URL": input_presigned_url,
            "OUTPUT_PRESIGNED_URL": output_presigned_url,
        })
        
        # Map params to environment variables
        param_mapping = {
            "debug": "DEBUG",
            "seed": "SEED",
            "color_correction": "COLOR_CORRECTION",
            "vae_model": "VAE_MODEL",
            "model": "MODEL",
            "resolution": "RESOLUTION",
            "batch_size_strategy": "BATCH_SIZE_STRATEGY",
            "batch_size_explicit": "BATCH_SIZE_EXPLICIT",
            "batch_size_conservative": "BATCH_SIZE_CONSERVATIVE",
            "batch_size_quality": "BATCH_SIZE_QUALITY",
            "chunk_size_strategy": "CHUNK_SIZE_STRATEGY",
            "chunk_size_explicit": "CHUNK_SIZE_EXPLICIT",
            "chunk_size_recommended": "CHUNK_SIZE_RECOMMENDED",
            "chunk_size_fallback": "CHUNK_SIZE_FALLBACK",
            "attention_mode": "ATTENTION_MODE",
            "temporal_overlap": "TEMPORAL_OVERLAP",
            "vae_encode_tiled": "VAE_ENCODE_TILED",
            "vae_decode_tiled": "VAE_DECODE_TILED",
            "cache_dit": "CACHE_DIT",
            "cache_vae": "CACHE_VAE",
        }
        
        for param_key, env_var in param_mapping.items():
            if param_key in params and params[param_key] is not None:
                env[env_var] = str(params[param_key])
        
        # Execute the shell script
        logger.info(f"Executing: {SCRIPT_PATH}")
        result = subprocess.run(
            ["/bin/bash", SCRIPT_PATH],
            env=env,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Parse metrics from stdout (look for [METRIC] lines)
        metrics = {}
        for line in result.stdout.splitlines():
            if "[METRIC]" in line:
                # Extract metric name and value
                # Format: [METRIC] 2025-12-29T12:34:56Z metric_name=value
                parts = line.split("=", 1)
                if len(parts) == 2:
                    metric_name = parts[0].split()[-1]
                    metric_value = parts[1].strip()
                    try:
                        # Try to convert to number
                        metrics[metric_name] = float(metric_value) if "." in metric_value else int(metric_value)
                    except ValueError:
                        metrics[metric_name] = metric_value
        
        if result.returncode != 0:
            logger.error(f"Script failed with exit code {result.returncode}")
            logger.error(f"STDOUT:\n{result.stdout}")
            logger.error(f"STDERR:\n{result.stderr}")
            
            return {
                "status": "error",
                "error": f"Script exited with code {result.returncode}",
                "stderr": result.stderr[-1000:],  # Last 1000 chars
                "duration_seconds": round(time.time() - start_time, 2)
            }
        
        total_duration = time.time() - start_time
        
        logger.info(f"Upscale completed successfully in {total_duration:.2f}s")
        
        return {
            "status": "success",
            "output_presigned_url": output_presigned_url,
            "metrics": {
                **metrics,
                "total_duration_seconds": round(total_duration, 2)
            }
        }
        
    except subprocess.TimeoutExpired:
        logger.error("Script execution timed out")
        return {
            "status": "error",
            "error": "Script execution timed out after 1 hour",
            "duration_seconds": round(time.time() - start_time, 2)
        }
    
    except Exception as e:
        logger.error(f"Error during upscaling: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "error": str(e),
            "duration_seconds": round(time.time() - start_time, 2)
        }


# RunPod handler
runpod.serverless.start({"handler": upscale_segment})
