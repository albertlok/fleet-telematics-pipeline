FROM python:3.12-slim

WORKDIR /app

# confluent-kafka needs librdkafka (C library) and gcc to install.
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Dependencies first, code second — keeps Docker's layer cache effective.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# -u = unbuffered output, so log lines appear in `kubectl logs`
# immediately instead of being held in a buffer.
CMD ["python", "-u", "main.py"]
