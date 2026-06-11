# Slim Python base image: much smaller than the full image, which means
# faster pulls and a smaller attack surface.
FROM python:3.12-slim

WORKDIR /app

# confluent-kafka is a C extension wrapping librdkafka, so we need the
# C library and a compiler available at install time.
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements FIRST and install, THEN copy the code. Docker caches
# each layer — this ordering means editing a .py file doesn't force a
# slow re-install of all dependencies on the next build.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# 4 uvicorn workers = 4 independent processes serving requests, making
# better use of the pod's CPU allowance.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
