FROM python:3.12-slim AS base

# Build dependencies for llama-cpp-python (C++ compilation)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV COPILOT_PORT=8081

EXPOSE 8081

CMD ["python", "main.py"]
