FROM nvidia/cuda:12.4.0-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv git ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install AWS CLI
RUN pip install --no-cache-dir awscli

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install PyTorch with CUDA support
RUN pip install --no-cache-dir --pre torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/cu121

# Install SeedVR2
ARG SEEDVR2_REPO="https://github.com/numz/ComfyUI-SeedVR2_VideoUpscaler.git"
ARG SEEDVR2_REF="v2.5.23"

RUN git clone "$SEEDVR2_REPO" /opt/seedvr2 \
    && cd /opt/seedvr2 \
    && git checkout "$SEEDVR2_REF" \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir .

# Run help command to verify installation
RUN python /opt/seedvr2/inference_cli.py --help

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy handler and shell script
COPY handler.py /app/
COPY upscale_segment.sh /app/
RUN chmod +x /app/upscale_segment.sh

# Create work directories
RUN mkdir -p /work/in /work/out /models

CMD ["python", "-u", "handler.py"]
