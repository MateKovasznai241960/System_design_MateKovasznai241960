# Containerised NO2 ingest for Luchtmeetnet station NL10240 -> PostgreSQL.
# Build from the PROJECT ROOT (task 004, D-034; the context is ".", the root
# .dockerignore allow-list decides which files the build can see):
#   docker build -t airbreda-air .
# Run (fetches live data, writes to the database; D-021):
#   docker run --rm -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGPASSWORD -e PGSSLMODE airbreda-air
#   ("-e NAME" without "=value" copies the value from your shell, so the
#   password is not typed on the command line.) Fetch-only, no database:
#   docker run --rm airbreda-air python ingest_air.py --no-db
# With Compose: docker compose up (reads the settings from .env).
# Default CMD (task 005): --serve, fetch every hour, GET /health on port 8001.
# Database settings are runtime environment variables ONLY. Never add ENV or
# ARG lines with PG* values here: they would be stored in the image layers.
FROM python:3.12-slim

# Unbuffered output so readings show up in container logs immediately.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first so this layer is cached when only code changes.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy only the code the script needs - no data files, no credentials.
COPY getNO2Readings.py ingest_air.py ./

# Run as an unprivileged user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

# Task 005 (D-041): long-running by default, GET /health on port 8001.
EXPOSE 8001
CMD ["python", "ingest_air.py", "--serve"]
