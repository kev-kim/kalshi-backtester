# Multi-stage build — Python 3.12 slim, non-root user
# Stage 1: build dependencies
FROM python:3.12-slim AS builder

WORKDIR /build

RUN pip install --no-cache-dir uv

COPY pyproject.toml .
COPY src/ src/

# Install into an isolated prefix so we can copy cleanly
RUN uv pip install --system --no-cache .


# Stage 2: runtime image
FROM python:3.12-slim AS runtime

# Security: non-root user
RUN groupadd --gid 1001 appgroup && \
    useradd --uid 1001 --gid appgroup --no-create-home --shell /bin/false appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application source
COPY src/ src/
COPY sql/ sql/
COPY scripts/ scripts/

# Private key mount point (mounted as Docker secret at runtime)
RUN mkdir -p /run/secrets && chown appuser:appgroup /run/secrets

USER appuser

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import asyncio,asyncpg,os; asyncio.run(asyncpg.connect(os.environ['POSTGRES_DSN']))" \
    || exit 1

ENTRYPOINT ["python", "src/main.py"]
CMD ["collector"]
