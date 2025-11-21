FROM nvidia/cuda:12.4.0-base-ubuntu22.04

WORKDIR /app

# Install Python, pip, and system dependencies
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    python3-setuptools \
    python3-dev \
    gcc \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    libgl1-mesa-glx \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Ensure python3 and pip are default
RUN ln -sf python3.10 /usr/bin/python && \
    ln -sf pip3 /usr/bin/pip

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PyTorch with CUDA 12.4 support
RUN pip uninstall torch torchaudio torchvision -y && \
    pip install torch==2.4.1 torchvision==0.19.1 --index-url https://download.pytorch.org/whl/cu124

# Clear pip cache to reduce image size
RUN pip cache purge

# Copy application files
COPY src/ ./src/
COPY Models/ ./Models/

# Set Python path
ENV PYTHONPATH=/app

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8100/health').raise_for_status()"

# Run application
CMD ["python", "-m", "src.main"]
