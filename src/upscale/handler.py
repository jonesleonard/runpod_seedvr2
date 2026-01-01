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
from urllib.request import urlopen, Request
import runpod
from tqdm import tqdm

level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, level, logging.INFO))
logger = logging.getLogger(__name__)

SCRIPT_PATH = "/app/upscale_segment.sh"
# Default directory where models should reside when not downloading.
# This aligns with the mounted network volume path used by the shell script.
MODELS_DEFAULT_DIR = "/runpod-volume/models"
_MODELS_READY = False
DOWNLOAD_CHUNK_SIZE = 1024 * 1024


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
    temp_path = Path(f"{dest_path}.download")
    existing_size = temp_path.stat().st_size if temp_path.exists() else 0
    headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}

    request = Request(url, headers=headers)
    with urlopen(request, timeout=60) as response:
        content_length = int(response.headers.get("Content-Length", 0) or 0)
        content_range = response.headers.get("Content-Range")

        if existing_size > 0 and not content_range:
            logger.info("Server did not honor Range; restarting download for %s", dest_path)
            existing_size = 0
            temp_path.unlink(missing_ok=True)

        total_size = existing_size + content_length if content_length else None
        mode = "ab" if existing_size else "wb"

        with open(temp_path, mode) as out_file:
            with tqdm(
                total=total_size,
                initial=existing_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest_path.name,
                disable=False,
            ) as pbar:
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    pbar.update(len(chunk))

    os.replace(temp_path, dest_path)
    logger.info("Downloaded %s (%s bytes)", dest_path, dest_path.stat().st_size)


def _ensure_models_downloaded(job_input: Dict[str, Any]) -> None:
    global _MODELS_READY
    if _MODELS_READY:
        return
    # Determine whether to download models based on job input or environment.
    should_download = False
    val = job_input.get("download_models") or job_input.get("params", {}).get("download_models")
    if val is not None:
        # Accept booleans or truthy strings
        should_download = bool(val) if isinstance(val, bool) else str(val).lower() in {"1", "true", "yes", "on"}
    else:
        env_val = os.environ.get("DOWNLOAD_MODELS")
        if env_val is not None:
            should_download = str(env_val).lower() in {"1", "true", "yes", "on"}

    models_dir = Path(os.environ.get("MODELS_DIR", MODELS_DEFAULT_DIR))
    models_dir.mkdir(parents=True, exist_ok=True)

    if not should_download:
        # No download requested; verify models exist in the default directory
        existing_files = list(models_dir.glob("*"))
        if not existing_files:
            raise ValueError(
                f"Models directory '{models_dir}' is empty. "
                "Provide presigned URLs and set 'download_models=true', "
                "or pre-populate the models on the network volume."
            )
        logger.info("Using pre-existing models from %s (%d files)", models_dir, len(existing_files))
        _MODELS_READY = True
        return

    # Download requested: require presigned URLs and fetch into models_dir
    urls = _get_model_urls(job_input)
    if not urls:
        raise ValueError(
            "download_models flag is set, but no model presigned URLs were provided. "
            "Expected keys: 'vae_model_presigned_url' and 'dit_model_presigned_url'."
        )

    for url in urls:
        filename = _filename_from_url(url)
        dest_path = models_dir / filename
        if dest_path.exists() and dest_path.stat().st_size > 0:
            logger.info("Model already present: %s", dest_path)
            continue
        _download_file(url, dest_path)

    _MODELS_READY = True


def _set_log_level(job_input: Dict[str, Any]) -> None:
    level = job_input.get("log_level") or job_input.get("params", {}).get("log_level")
    if not level:
        return
    level_name = str(level).upper()
    new_level = getattr(logging, level_name, None)
    if new_level is None:
        logger.warning("Unknown log level '%s'; using default.", level)
        return
    logger.setLevel(new_level)
    logging.getLogger().setLevel(new_level)


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
    
    Models are loaded from local storage (default: /workspace/models) unless
    the 'download_models' flag is set (job_input param or DOWNLOAD_MODELS env),
    in which case models are downloaded from provided presigned URLs.
    """
    start_time = time.time()
    
    try:

        # debug log full job
        logger.debug(f"Received job: {json.dumps(job, indent=2)}")

        # Support both "input" wrapper and direct job dict
        job_input = job.get("input", job)

        # debug log job input
        logger.debug(f"Received job input: {json.dumps(job_input, indent=2)}")

        _set_log_level(job_input)

        # Validate input - now using presigned URLs instead of S3 URIs
        input_presigned_url = job_input.get("input_presigned_url")
        output_presigned_url = job_input.get("output_presigned_url")
        vae_model_presigned_url = job_input.get("vae_model_presigned_url")
        dit_model_presigned_url = job_input.get("dit_model_presigned_url")
        params = job_input.get("params", {})
        
        if not input_presigned_url:
            raise ValueError("input_presigned_url is required")
        if not output_presigned_url:
            raise ValueError("output_presigned_url is required")
        # Model URLs are only required when download flag is enabled; _ensure_models_downloaded handles this.

        _ensure_models_downloaded(job_input)
        
        logger.info("Starting upscale job with presigned URLs")
        
        # Build environment variables for the shell script
        env = os.environ.copy()
        env.update({
            "INPUT_PRESIGNED_URL": input_presigned_url,
        })
        if output_presigned_url:
            env["OUTPUT_PRESIGNED_URL"] = output_presigned_url
        
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
        process = subprocess.Popen(
            ["/bin/bash", SCRIPT_PATH],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        metrics = {}
        output_lines = []

        try:
            for line in process.stdout:
                output_lines.append(line)
                logger.info(line.rstrip())
                if "[METRIC]" in line:
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        metric_name = parts[0].split()[-1]
                        metric_value = parts[1].strip()
                        try:
                            metrics[metric_name] = (
                                float(metric_value) if "." in metric_value else int(metric_value)
                            )
                        except ValueError:
                            metrics[metric_name] = metric_value
        finally:
            process.stdout.close()

        return_code = process.wait(timeout=3600)

        if return_code != 0:
            logger.error("Script failed with exit code %s", return_code)
            
            return {
                "status": "error",
                "error": f"Script exited with code {return_code}",
                "stderr": "".join(output_lines)[-1000:],  # Last 1000 chars
                "duration_seconds": round(time.time() - start_time, 2)
            }
        
        total_duration = time.time() - start_time
        
        logger.info(f"Upscale completed successfully in {total_duration:.2f}s")
        
        return {
            "status": "success",
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
