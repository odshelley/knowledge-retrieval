FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv==0.5.7

WORKDIR /opt/code

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

ENV DAGSTER_HOME=/opt/dagster_home
ENV PYTHONPATH=/opt/code

CMD ["uv", "run", "dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster_home/workspace.yaml"]
