FROM python:3.13.2-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY recnode recnode

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv sync --no-cache --frozen --no-dev

# Runtime Stage
FROM python:3.13.2-alpine

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/recnode /app/recnode

ENV PATH="/app/.venv/bin:$PATH"
