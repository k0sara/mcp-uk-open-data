FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir mcp pydantic requests
COPY src /app/src
COPY scripts /app/scripts
ENV PYTHONPATH=/app/src
ENTRYPOINT ["python", "-m", "mcp_uk_open_data.server"]
