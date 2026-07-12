FROM python:3.13-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
	curl \
	&& rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.12 /uv /uvx /usr/local/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
	UV_LINK_MODE=copy \
	PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project --no-group dev --no-group notebook

COPY src ./src
COPY alembic.ini ./alembic.ini
COPY migrations ./migrations
COPY README.md LICENSE ./
RUN uv sync --locked --no-group dev --no-group notebook

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["deepsky-api"]
