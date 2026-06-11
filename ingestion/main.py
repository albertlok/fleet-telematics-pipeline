"""
Ingestion Layer — the "front door" of the pipeline.

This is a small, stateless web service whose ONLY job is to:
  1. Receive webhook POSTs from the telematics provider (GPS pings,
     driver safety events, engine fault codes, etc.).
  2. Check the payload really came from the provider (signature check).
  3. Drop the payload onto a Kafka topic and return "200 OK" immediately.

Why so minimal? Webhook senders typically expect a fast response (a few
seconds at most) or they will consider the delivery failed and retry.
By doing NO heavy work here — no database writes, no transformation —
we can always answer quickly, even during traffic spikes. The slow work
happens later, in the processor service, at its own pace.

"Stateless" means this service keeps nothing important in memory between
requests, so Kubernetes can freely add or remove copies of it (scale
out/in) without losing data.
"""

import hashlib
import hmac
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, status
from confluent_kafka import Producer, KafkaException

from config import Settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Settings are read from environment variables (see config.py).
# In Kubernetes, those env vars come from the ConfigMap and Secret.
settings = Settings()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs at startup (before the yield) and shutdown (after the yield).
    On shutdown we flush the Kafka producer so any messages still
    buffered in memory are delivered before the process exits.
    """
    yield
    if _producer:
        _producer.flush(timeout=10)


app = FastAPI(title="Telemetry Ingestion Service", version="1.0.0", lifespan=lifespan)

# The Kafka producer is created lazily (on first request) and then reused.
# A Producer maintains network connections to the Kafka brokers, which are
# expensive to set up, so we create it once and share it.
_producer: Producer | None = None


def get_producer() -> Producer:
    """Return the shared Kafka producer, creating it on first use."""
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                # acks=all: wait until every in-sync broker replica has the
                # message before considering it "sent". Slightly slower, but
                # we never lose a message just because one broker died.
                "acks": "all",
                "retries": 5,
                "retry.backoff.ms": 300,
                # Idempotence: if a retry accidentally sends the same message
                # twice, Kafka deduplicates it on the broker side.
                "enable.idempotence": True,
            }
        )
    return _producer


def verify_signature(secret: str, raw_body: bytes, signature_header: str) -> bool:
    """
    Verify the webhook's HMAC-SHA256 signature.

    How it works: the provider and we share a secret key. The provider
    computes HMAC-SHA256(secret, request_body) and sends the result in a
    header. We compute the same thing locally — if the values match, the
    payload genuinely came from the provider and wasn't tampered with.

    We use hmac.compare_digest (not ==) because it takes constant time
    regardless of where the strings differ. A plain == comparison can leak
    timing information that helps attackers guess the signature.
    """
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature_header)


def _delivery_report(err, msg):
    """Kafka calls this back asynchronously for every produced message."""
    if err:
        log.error("Kafka delivery failed: %s", err)
    else:
        log.debug(
            "Delivered to %s [partition %d] @ offset %d",
            msg.topic(), msg.partition(), msg.offset(),
        )


@app.get("/healthz")
def health():
    """
    Health check endpoint. Kubernetes calls this regularly to decide
    whether the pod is alive (liveness probe) and ready to receive
    traffic (readiness probe).
    """
    return {"status": "ok", "ts": int(time.time())}


@app.post("/webhooks/telemetry", status_code=status.HTTP_200_OK)
async def receive_webhook(
    request: Request,
    x_webhook_signature: str | None = Header(default=None),
):
    """
    The main webhook endpoint. The flow is:

      verify signature → parse JSON → wrap in an envelope → publish to Kafka

    Note we read the RAW request body (bytes) for the signature check.
    The signature was computed over the exact bytes sent, so we must
    verify against those bytes BEFORE parsing the JSON — parsing and
    re-serializing could change whitespace/key order and break the check.
    """
    raw_body = await request.body()

    # Signature verification is skipped when no secret is configured
    # (convenient for local development; ALWAYS set a secret in production).
    if settings.webhook_secret:
        if not x_webhook_signature:
            raise HTTPException(status_code=401, detail="Missing signature header")
        if not verify_signature(settings.webhook_secret, raw_body, x_webhook_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        # 400 = "your request is malformed, don't retry it as-is"
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # Wrap the payload in an "envelope" with our own metadata. The
    # ingestion_id lets us trace one event through every later stage,
    # and ingested_at records when WE received it (the event itself may
    # have happened earlier — devices buffer data when out of cell coverage).
    envelope = {
        "ingestion_id": str(uuid.uuid4()),
        "ingested_at": int(time.time() * 1000),  # epoch milliseconds
        "payload": payload,
    }

    producer = get_producer()
    try:
        producer.produce(
            topic=settings.kafka_topic_raw,
            # The key controls which Kafka partition the message lands on.
            # Messages with the same key always go to the same partition,
            # which preserves their ordering relative to each other.
            key=str(payload.get("eventId") or envelope["ingestion_id"]).encode(),
            value=json.dumps(envelope).encode(),
            callback=_delivery_report,
        )
        # poll(0) gives the producer a chance to run delivery callbacks.
        # It does NOT block waiting for delivery — produce() is async.
        producer.poll(0)
    except (KafkaException, BufferError) as exc:
        log.exception("Failed to publish to Kafka")
        # 503 tells the provider "temporary problem, please retry later".
        raise HTTPException(status_code=503, detail="Broker unavailable") from exc

    return {"status": "accepted", "ingestion_id": envelope["ingestion_id"]}
