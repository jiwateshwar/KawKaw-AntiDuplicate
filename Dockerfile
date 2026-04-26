FROM python:3.12-slim

# System deps: libheif for HEIC/HEIF support, curl for healthcheck, psycopg2 for sync DB access
RUN apt-get update && apt-get install -y --no-install-recommends \
    libheif-dev \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir psycopg2-binary

COPY app/ ./app/
COPY icon.png ./static/icon.png

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV DATABASE_URL=postgresql+asyncpg://kawkaw:secret@db:5432/kawkaw
ENV SCAN_FOLDERS=/mnt/photos
ENV SCAN_SCHEDULE=02:30
ENV TRASH_FOLDER=/mnt/photos/.trash
ENV TZ=UTC

EXPOSE 3681

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -sf http://localhost:3681/api/dashboard || exit 1

# --workers 1 required: APScheduler and scan threading.Lock are not safe across multiple worker processes
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3681", "--workers", "1"]
