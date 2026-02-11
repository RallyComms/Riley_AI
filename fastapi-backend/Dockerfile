FROM python:3.11-slim

# System dependencies for Office-to-PDF preview generation
RUN apt-get update && apt-get install -y --no-install-recommends \
    libreoffice \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python -m pip install --upgrade pip


# Install Python dependencies first for build caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure static directory exists in the image
RUN mkdir -p /app/static

ENV PORT=8080

# Cloud Run expects the container to listen on $PORT
CMD exec gunicorn -k uvicorn.workers.UvicornWorker -w 2 -b 0.0.0.0:${PORT} app.main:app

