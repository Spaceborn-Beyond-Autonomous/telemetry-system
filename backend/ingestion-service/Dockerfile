# ---------------------------------------------------------------------------
# Ingestion Service — Multi-stage Docker Build
# ---------------------------------------------------------------------------

# Stage 1: Build dependencies
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Stage 2: Production image
FROM python:3.12-slim AS runtime

# Security: run as non-root user
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid 1001 --shell /bin/false appuser

WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy shared library (needed at runtime)
COPY ../../shared /app/shared

# Copy application code
COPY app/ /app/app/

# Set Python path so shared modules are importable
ENV PYTHONPATH="/app"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Default environment variables (overridable at runtime)
ENV INGESTION_SERVICE_HOST="0.0.0.0"
ENV INGESTION_SERVICE_PORT="8001"
ENV INGESTION_KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
ENV INGESTION_KAFKA_TOPIC="telemetry.raw"
ENV INGESTION_LOG_LEVEL="INFO"
ENV INGESTION_LOG_JSON="true"

EXPOSE 8001

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8001/api/v1/health')" || exit 1

# Run with uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1", "--loop", "uvloop", "--http", "httptools"]
