# ============================================================
# Stage 1 — Build dependencies
# ============================================================
FROM python:3.12-slim AS builder

WORKDIR /build

COPY backend/raw-writer-service/requirements.txt .
RUN pip install --no-cache-dir --target=/build/deps -r requirements.txt

# ============================================================
# Stage 2 — Production image
# ============================================================
FROM python:3.12-slim

LABEL maintainer="telemetry-system"
LABEL service="raw-writer-service"

# Non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Install dependencies from builder
COPY --from=builder /build/deps /usr/local/lib/python3.12/site-packages/

# Copy shared foundation
COPY shared/ /app/shared/

# Copy service code
COPY backend/raw-writer-service/app/ /app/app/

# Set Python path so shared imports work
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Default env vars (overridable at runtime)
ENV RAW_WRITER_KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
ENV RAW_WRITER_DB_HOST="timescaledb"
ENV RAW_WRITER_DB_PORT="5432"
ENV RAW_WRITER_DB_NAME="telemetry"
ENV RAW_WRITER_DB_USER="telemetry"
ENV RAW_WRITER_DB_PASSWORD="telemetry"
ENV RAW_WRITER_LOG_JSON="true"
ENV RAW_WRITER_LOG_LEVEL="INFO"

USER appuser

CMD ["python", "-m", "app.main"]
