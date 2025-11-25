FROM python:3.13.2-alpine AS builder

WORKDIR /app

# - cargo & rust: Rust 컴파일러
# - gcc, musl-dev, libffi-dev, openssl-dev: Python/Rust 라이브러리 컴파일용 헤더
RUN apk add --no-cache \
    git \
    cargo \
    rust \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    patchelf

COPY pyproject.toml .
COPY uv.lock .
COPY README.md .
COPY Cargo.toml .
COPY src src
COPY recnode recnode

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv sync --no-cache --frozen --no-dev

# Runtime Stage
FROM python:3.13.2-alpine

WORKDIR /app

RUN apk add --no-cache git libgcc

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/recnode /app/recnode

ENV PATH="/app/.venv/bin:$PATH"
