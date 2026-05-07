
FROM pytorch/pytorch:2.7.1-cuda11.8-cudnn9-runtime

# OS libs required by OpenCV + psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
        libglib2.0-0 \
        libgl1 \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python dependencies ───────────────────────────────────────────────────────
COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

# ── Source code ───────────────────────────────────────────────────────────────
COPY . .

# Weights and ROI config are mounted via docker-compose volumes —
# do NOT bake them into the image.

CMD ["python", "main.py"]