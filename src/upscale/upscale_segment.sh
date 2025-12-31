#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Video Upscaler - Upscale a single video segment using SeedVR2
# ============================================================================

readonly WORK_DIR="/work"
readonly INPUT_DIR="${WORK_DIR}/in"
readonly OUTPUT_DIR="${WORK_DIR}/out"
readonly MODEL_DIR="${MODELS_DIR:-/models}"  # Local model storage (can override)
readonly INPUT_FILE="${INPUT_DIR}/segment.mp4"
readonly OUTPUT_FILE="${OUTPUT_DIR}/up.mp4"

cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        echo "============================================================================"
        echo "ERROR: Script failed with exit code $exit_code"
        echo "============================================================================"
    fi
    # Cleanup work directory
    if [[ -d "$WORK_DIR" ]]; then
        echo "Cleaning up work directory..."
        rm -rf "${INPUT_DIR:?}"/* "${OUTPUT_DIR:?}"/* 2>/dev/null || true
    fi
}

trap cleanup EXIT

log_info() {
    echo "[INFO] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"
}

log_error() {
    echo "[ERROR] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*" >&2
}

log_metric() {
    local metric_name=$1
    local metric_value=$2
    echo "[METRIC] $(date -u +"%Y-%m-%dT%H:%M:%SZ") ${metric_name}=${metric_value}"
}

validate_requirements() {
    log_info "Validating requirements..."
    
    # Check required commands
    local required_commands=(curl python3 ffmpeg)
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "Required command not found: $cmd"
            exit 1
        fi
    done
    
    # Validate required environment variables - now using presigned URLs
    if [[ -z "${INPUT_PRESIGNED_URL:-}" ]]; then
        log_error "INPUT_PRESIGNED_URL is required"
        exit 1
    fi
    
    if [[ -z "${OUTPUT_PRESIGNED_URL:-}" ]]; then
        log_error "OUTPUT_PRESIGNED_URL is required"
        exit 1
    fi
    
    # Validate presigned URL format (must be HTTPS)
    if [[ ! "$INPUT_PRESIGNED_URL" =~ ^https:// ]]; then
        log_error "INPUT_PRESIGNED_URL must be an HTTPS URL"
        exit 1
    fi
    
    if [[ ! "$OUTPUT_PRESIGNED_URL" =~ ^https:// ]]; then
        log_error "OUTPUT_PRESIGNED_URL must be an HTTPS URL"
        exit 1
    fi
    
    # Validate inference_cli.py is available
    if ! python3 -c "import sys; sys.path.insert(0, '/opt/seedvr2'); import inference_cli" 2>/dev/null; then
        log_error "SeedVR2 inference_cli module not found. Check installation."
        exit 1
    fi
    
    log_info "All requirements validated"
}

main() {
    # Set defaults for SeedVR2 parameters
    readonly DEBUG="${DEBUG:-false}"
    readonly SEED="${SEED:-42}"
    readonly COLOR_CORRECTION="${COLOR_CORRECTION:-lab}"
    readonly MODEL="${MODEL:-7b}"
    readonly RESOLUTION="${RESOLUTION:-1080}"
    readonly ATTENTION_MODE="${ATTENTION_MODE:-}"
    readonly TEMPORAL_OVERLAP="${TEMPORAL_OVERLAP:-}"
    readonly VAE_ENCODE_TILED="${VAE_ENCODE_TILED:-}"
    readonly VAE_DECODE_TILED="${VAE_DECODE_TILED:-}"
    readonly CACHE_DIT="${CACHE_DIT:-}"
    readonly CACHE_VAE="${CACHE_VAE:-}"
    
    # Batch size strategy: "conservative", "quality", or "explicit"
    readonly BATCH_SIZE_STRATEGY="${BATCH_SIZE_STRATEGY:-explicit}"
    readonly BATCH_SIZE_EXPLICIT="${BATCH_SIZE_EXPLICIT:-129}"
    readonly BATCH_SIZE_CONSERVATIVE="${BATCH_SIZE_CONSERVATIVE:-}"
    readonly BATCH_SIZE_QUALITY="${BATCH_SIZE_QUALITY:-}"
    
    # Resolve batch_size based on strategy
    local batch_size
    case "$BATCH_SIZE_STRATEGY" in
        conservative)
            if [[ -n "$BATCH_SIZE_CONSERVATIVE" && "$BATCH_SIZE_CONSERVATIVE" != "null" ]]; then
                batch_size="$BATCH_SIZE_CONSERVATIVE"
            else
                log_error "BATCH_SIZE_STRATEGY=conservative but BATCH_SIZE_CONSERVATIVE not provided"
                exit 1
            fi
            ;;
        quality)
            if [[ -n "$BATCH_SIZE_QUALITY" && "$BATCH_SIZE_QUALITY" != "null" ]]; then
                batch_size="$BATCH_SIZE_QUALITY"
            else
                log_error "BATCH_SIZE_STRATEGY=quality but BATCH_SIZE_QUALITY not provided"
                exit 1
            fi
            ;;
        explicit|*)
            batch_size="$BATCH_SIZE_EXPLICIT"
            ;;
    esac
    readonly BATCH_SIZE="$batch_size"

    # Chunk size strategy: "recommended", "fallback", or "explicit"
    readonly CHUNK_SIZE_STRATEGY="${CHUNK_SIZE_STRATEGY:-explicit}"
    readonly CHUNK_SIZE_EXPLICIT="${CHUNK_SIZE_EXPLICIT:-}"
    readonly CHUNK_SIZE_RECOMMENDED="${CHUNK_SIZE_RECOMMENDED:-}"
    readonly CHUNK_SIZE_FALLBACK="${CHUNK_SIZE_FALLBACK:-}"

    # Resolve chunk_size based on strategy
    local chunk_size
    case "$CHUNK_SIZE_STRATEGY" in
        recommended)
            if [[ -n "$CHUNK_SIZE_RECOMMENDED" && "$CHUNK_SIZE_RECOMMENDED" != "null" ]]; then
                chunk_size="$CHUNK_SIZE_RECOMMENDED"
            else
                log_error "CHUNK_SIZE_STRATEGY=recommended but CHUNK_SIZE_RECOMMENDED not provided"
                exit 1
            fi
            ;;
        fallback)
            if [[ -n "$CHUNK_SIZE_FALLBACK" && "$CHUNK_SIZE_FALLBACK" != "null" ]]; then
                chunk_size="$CHUNK_SIZE_FALLBACK"
            else
                log_error "CHUNK_SIZE_STRATEGY=fallback but CHUNK_SIZE_FALLBACK not provided"
                exit 1
            fi
            ;;
        explicit|*)
            chunk_size="$CHUNK_SIZE_EXPLICIT"
            ;;
    esac
    readonly CHUNK_SIZE="$chunk_size"
    
    echo "============================================================================"
    echo "Video Upscaler - Starting"
    echo "============================================================================"
    log_info "Configuration:"
    log_info "  MODEL_DIR: $MODEL_DIR"
    log_info "  DEBUG: $DEBUG"
    log_info "  SEED: $SEED"
    log_info "  COLOR_CORRECTION: $COLOR_CORRECTION"
    log_info "  MODEL: $MODEL"
    log_info "  RESOLUTION: $RESOLUTION"
    log_info "  BATCH_SIZE_STRATEGY: $BATCH_SIZE_STRATEGY"
    log_info "  BATCH_SIZE: $BATCH_SIZE (resolved)"
    log_info "  CHUNK_SIZE_STRATEGY: $CHUNK_SIZE_STRATEGY"
    [[ -n "$CHUNK_SIZE" ]] && log_info "  CHUNK_SIZE: $CHUNK_SIZE (resolved)"
    [[ -n "$ATTENTION_MODE" ]] && log_info "  ATTENTION_MODE: $ATTENTION_MODE"
    [[ -n "$TEMPORAL_OVERLAP" ]] && log_info "  TEMPORAL_OVERLAP: $TEMPORAL_OVERLAP"
    [[ -n "$VAE_ENCODE_TILED" ]] && log_info "  VAE_ENCODE_TILED: $VAE_ENCODE_TILED"
    [[ -n "$VAE_DECODE_TILED" ]] && log_info "  VAE_DECODE_TILED: $VAE_DECODE_TILED"
    [[ -n "$CACHE_DIT" ]] && log_info "  CACHE_DIT: $CACHE_DIT"
    [[ -n "$CACHE_VAE" ]] && log_info "  CACHE_VAE: $CACHE_VAE"
    echo "============================================================================"
    
    validate_requirements
    
    # Install sageattention if needed
    if [[ "$ATTENTION_MODE" == "sageattn_2" ]] || [[ "$ATTENTION_MODE" == "sageattn_3" ]]; then
        log_info "ATTENTION_MODE=$ATTENTION_MODE detected. Installing sageattention..."
        if ! pip3 install --no-cache-dir sageattention; then
            log_error "Failed to install sageattention"
            exit 1
        fi
        log_info "sageattention installed successfully"
    fi
    
    # Create work directories
    log_info "Creating work directories..."
    mkdir -p "$INPUT_DIR" "$OUTPUT_DIR"
    
    # Check models exist locally (pre-populated or downloaded at startup)
    local model_count
    model_count=$(find "$MODEL_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
    
    if [[ "$model_count" -eq 0 ]]; then
        log_error "No models found at $MODEL_DIR"
        log_error "Models must be present before running jobs."
        exit 1
    fi
    
    log_info "Using models from $MODEL_DIR ($model_count files found)"
    log_metric "model_file_count" "$model_count"
    
    # Download input segment using presigned URL
    log_info "Downloading input segment..."
    local download_start
    download_start=$(date +%s)
    
    if ! curl -fsSL --retry 3 --retry-delay 2 -o "$INPUT_FILE" "$INPUT_PRESIGNED_URL"; then
        log_error "Failed to download input segment from presigned URL"
        exit 1
    fi
    
    local download_end
    download_end=$(date +%s)
    local download_duration=$((download_end - download_start))
    log_info "Input downloaded in ${download_duration} seconds"
    log_metric "input_download_duration_seconds" "$download_duration"
    
    # Verify file was downloaded
    if [[ ! -f "$INPUT_FILE" ]]; then
        log_error "Input file does not exist after download: $INPUT_FILE"
        exit 1
    fi
    
    local file_size
    file_size=$(stat -f%z "$INPUT_FILE" 2>/dev/null || stat -c%s "$INPUT_FILE" 2>/dev/null || echo "unknown")
    log_info "Downloaded segment file size: ${file_size} bytes"
    log_metric "input_segment_size_bytes" "$file_size"
    
    # Run SeedVR2 upscaling
    log_info "Starting SeedVR2 upscaling..."
    local start_time
    start_time=$(date +%s)
    
    # Build inference command with optional parameters
    local inference_cmd=(
        python3 /opt/seedvr2/inference_cli.py "$INPUT_FILE"
        --output "$OUTPUT_DIR"
        --model_dir "$MODEL_DIR"
        --dit_model ${MODEL}
        --resolution "$RESOLUTION"
        --batch_size "$BATCH_SIZE"
        --seed "$SEED"
        --color_correction "$COLOR_CORRECTION"
    )
    
    # Add optional parameters if set
    [[ "$DEBUG" == "true" ]] && inference_cmd+=(--debug)
    [[ -n "$ATTENTION_MODE" ]] && inference_cmd+=(--attention_mode "$ATTENTION_MODE")
    [[ -n "$CHUNK_SIZE" ]] && inference_cmd+=(--chunk_size "$CHUNK_SIZE")
    [[ -n "$TEMPORAL_OVERLAP" ]] && inference_cmd+=(--temporal_overlap "$TEMPORAL_OVERLAP")
    [[ "$VAE_ENCODE_TILED" == "true" ]] && inference_cmd+=(--vae_encode_tiled)
    [[ "$VAE_DECODE_TILED" == "true" ]] && inference_cmd+=(--vae_decode_tiled)
    [[ "$CACHE_DIT" == "true" ]] && inference_cmd+=(--cache_dit)
    [[ "$CACHE_VAE" == "true" ]] && inference_cmd+=(--cache_vae)
    
    if ! "${inference_cmd[@]}"; then
        log_error "SeedVR2 upscaling failed"
        exit 1
    fi
    
    local end_time
    end_time=$(date +%s)
    local duration=$((end_time - start_time))
    log_info "Upscaling completed in ${duration} seconds"
    log_metric "upscale_duration_seconds" "$duration"
    
    # Verify output file was created
    if [[ ! -f "$OUTPUT_FILE" ]]; then
        log_error "Output file was not created: $OUTPUT_FILE"
        exit 1
    fi
    
    local output_size
    output_size=$(stat -f%z "$OUTPUT_FILE" 2>/dev/null || stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo "unknown")
    log_info "Output segment file size: ${output_size} bytes"
    log_metric "output_segment_size_bytes" "$output_size"
    
    # Upload upscaled segment using presigned URL
    log_info "Uploading upscaled segment..."
    local upload_start
    upload_start=$(date +%s)
    
    if ! curl -fsSL --retry 3 --retry-delay 2 -X PUT -T "$OUTPUT_FILE" -H "Content-Type: video/mp4" "$OUTPUT_PRESIGNED_URL"; then
        log_error "Failed to upload segment using presigned URL"
        exit 1
    fi
    
    local upload_end
    upload_end=$(date +%s)
    local upload_duration=$((upload_end - upload_start))
    log_info "Output uploaded in ${upload_duration} seconds"
    log_metric "output_upload_duration_seconds" "$upload_duration"
    
    echo "============================================================================"
    echo "✅ Video upscaling completed successfully"
    echo "============================================================================"
    log_info "Summary:"
    log_info "  Processing time: ${duration}s"
    log_info "  Input size: ${file_size} bytes"
    log_info "  Output size: ${output_size} bytes"
    log_info "  Upload time: ${upload_duration}s"
    log_metric "job_status" "success"
    echo "============================================================================"
}

main "$@"
