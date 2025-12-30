FROM nvidia/cuda:13.1.0-runtime-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip git ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI
RUN pip3 install --no-cache-dir --root-user-action=ignore awscli

# Upgrade pip
RUN pip3 install --no-cache-dir --root-user-action=ignore --upgrade pip setuptools wheel

# Install PyTorch with CUDA support
RUN pip3 install --no-cache-dir --root-user-action=ignore --pre torch torchvision torchaudio \
    --index-url https://download.pytorch.org/whl/cu121

# Install SeedVR2
ARG SEEDVR2_REPO="https://github.com/numz/ComfyUI-SeedVR2_VideoUpscaler.git"
ARG SEEDVR2_REF="v2.5.23"

RUN git clone "$SEEDVR2_REPO" /opt/seedvr2 \
    && cd /opt/seedvr2 \
    && git checkout "$SEEDVR2_REF" \
    && pip3 install --no-cache-dir --root-user-action=ignore -r requirements.txt \
    && pip3 install --no-cache-dir --root-user-action=ignore .

WORKDIR /app

# Copy requirements and install
COPY requirements.txt /app/
RUN pip3 install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Copy handler and shell script
COPY handler.py /app/
COPY upscale_segment.sh /app/
RUN chmod +x /app/upscale_segment.sh

# Create work directories
RUN mkdir -p /work/in /work/out /models

CMD ["python3", "-u", "handler.py"]