FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    RAIN_DATA_DIR=/data

# tini reaps the cleaning/encoding child processes correctly on shutdown.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# System deps first for better layer caching; pyarrow wheels are self-contained.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

# Non-root runtime user; the data volume is chowned at startup via entrypoint.
RUN useradd --create-home --uid 10001 appuser && mkdir -p /data && chown -R appuser:appuser /data /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status==200 else 1)"

ENTRYPOINT ["/usr/bin/tini", "--", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
