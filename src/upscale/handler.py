"""
RunPod serverless handler for video upscaling using SeedVR2.
Delegates to upscale_segment.sh for the actual work.
"""

import os
import subprocess
import logging
import time
import json
from typing import Dict, Any
import runpod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCRIPT_PATH = "/app/upscale_segment.sh"


def upscale_segment(job_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upscale a single video segment by invoking the shell script.
    
    Expected job_input:
    {
        "input_s3_uri": "s3://bucket/path/to/segment.mp4",
        "output_s3_uri": "s3://bucket/path/to/output.mp4",
        "params": {
            "model": "7b",
            "resolution": 1080,
            "batch_size": 129,
            "seed": 42,
            ...
        }
    }
    """
    start_time = time.time()
    
    try:
        # Validate input
        input_s3_uri = job_input.get("input_s3_uri")
        output_s3_uri = job_input.get("output_s3_uri")
        params = job_input.get("params", {})
        
        if not input_s3_uri or not output_s3_uri:
            raise ValueError("input_s3_uri and output_s3_uri are required")
        
        logger.info(f"Starting upscale job: {input_s3_uri} -> {output_s3_uri}")

        if "models_s3_prefix" not in params or not params["models_s3_prefix"]:
            raise ValueError("params.models_s3_prefix is required")
        
        # Build environment variables for the shell script
        env = os.environ.copy()
        env.update({
            "INPUT_SEGMENT_S3_URI": input_s3_uri,
            "OUTPUT_SEGMENT_S3_URI": output_s3_uri,
            "MODELS_S3_PREFIX": params.get("models_s3_prefix")
        })
        
        # Map params to environment variables
        param_mapping = {
            "debug": "DEBUG",
            "seed": "SEED",
            "color_correction": "COLOR_CORRECTION",
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
            "output_s3_uri": output_s3_uri,
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