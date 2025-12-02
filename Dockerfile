# ---------- Build Stage ----------
FROM python:3.13-bookworm as builder

ENV PATH="/root/.local/bin:$PATH"
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --frozen --no-dev

COPY src .

# ---------- Final Runtime Stage ----------
FROM python:3.13-slim

WORKDIR /app

# enable venv
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# copy .venv runtime
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app .
