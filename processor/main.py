"""
Stream Processing Worker — the engine room of the pipeline.

This service runs an endless loop:

    poll Kafka → dedup check → transform → collect into a batch
                                                │
                       every 500 events or 2 seconds
                                                ▼
                                   bulk-write batch to PostgreSQL

Messages that can't be processed (malformed JSON, unknown event type,
transform errors) go to a Dead Letter Queue (DLQ) — a separate Kafka
topic where a human can inspect them later — instead of crashing or
blocking the pipeline.

DELIVERY GUARANTEE: at-least-once. We only "commit" our Kafka offsets
(i.e. tell Kafka "we're done with these messages") AFTER a batch has been
safely written to the database. If this pod crashes mid-batch, Kafka
redelivers the uncommitted messages to another pod. The cost of that
safety is occasional re-processing of the same event — which is exactly
why the dedup cache and the database's ON CONFLICT clause exist.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from config import Settings
from db_writer import DBWriter
from deduplication import DeduplicationCache
from transformer import transform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

settings = Settings()

# Flag flipped by the signal handler below; the main loop checks it
# every iteration so we can exit cleanly instead of mid-write.
_running = True


def _shutdown_handler(signum, frame):
    """
    Kubernetes sends SIGTERM when it wants a pod to stop (during
    deployments, scale-downs, node drains). We catch it, finish the
    current batch, commit offsets, and exit — no data is lost.
    The deployment grants 60s grace (terminationGracePeriodSeconds).
    """
    global _running
    log.info("Received shutdown signal — draining and stopping")
    _running = False


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)  # Ctrl-C when run locally


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # All pods sharing one group.id form a "consumer group":
            # Kafka splits the topic's partitions among them, so adding
            # pods automatically spreads the load.
            "group.id": settings.kafka_consumer_group,
            # If this group has never run before, start from the oldest
            # available message rather than skipping history.
            "auto.offset.reset": "earliest",
            # CRITICAL: we commit offsets manually, only after the batch
            # is safely in the database. Auto-commit would mark messages
            # "done" before we've actually stored them.
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 45_000,
        }
    )


def build_dlq_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "1",
        }
    )


def send_to_dlq(producer: Producer, envelope: dict, reason: str):
    """Park an unprocessable message on the DLQ topic with the reason why."""
    msg = json.dumps({"reason": reason, "envelope": envelope}).encode()
    producer.produce(topic=settings.kafka_topic_dlq, value=msg)
    producer.poll(0)


def run():
    consumer = build_consumer()
    dlq_producer = build_dlq_producer()
    dedup = DeduplicationCache(settings.redis_url)
    writer = DBWriter(settings.db_dsn)

    consumer.subscribe([settings.kafka_topic_raw])
    log.info("Subscribed to %s", settings.kafka_topic_raw)

    batch: list[dict[str, Any]] = []
    last_flush = time.monotonic()

    try:
        while _running:
            # poll() returns one message, or None if nothing arrived
            # within the timeout. The 1-second timeout is what lets the
            # loop notice the shutdown flag and the flush timer.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass  # quiet moment — fall through to the flush check
            elif msg.error():
                # _PARTITION_EOF just means "you've read everything so
                # far" — not a real error.
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Consumer error: %s", msg.error())
            else:
                _handle_message(msg, batch, dedup, dlq_producer)

            # Flush when the batch is full OR enough time has passed.
            # The time trigger matters during quiet periods: without it,
            # a half-full batch could sit in memory for hours.
            elapsed = time.monotonic() - last_flush
            if len(batch) >= settings.batch_size or elapsed >= settings.batch_flush_seconds:
                if batch:
                    _flush(batch, writer, dlq_producer)
                    batch.clear()
                # Commit AFTER the flush succeeds (or messages were
                # dead-lettered). This also covers skipped messages
                # (duplicates, DLQ'd) consumed since the last commit.
                _commit(consumer)
                last_flush = time.monotonic()

        # Graceful shutdown: write whatever is still in the batch.
        if batch:
            _flush(batch, writer, dlq_producer)
        _commit(consumer)

    finally:
        consumer.close()
        dlq_producer.flush(10)
        dedup.close()
        writer.close()
        log.info("Worker shut down cleanly")


def _handle_message(msg, batch: list, dedup: DeduplicationCache, dlq_producer: Producer):
    """Validate, dedup, and transform one Kafka message into the batch."""
    try:
        envelope = json.loads(msg.value().decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.error("Malformed message: %s", exc)
        send_to_dlq(dlq_producer, {"raw": repr(msg.value())}, f"json_decode_error: {exc}")
        return

    # Prefer the provider's eventId for dedup (it's stable across the
    # provider's own retries); fall back to our ingestion_id.
    event_id = (envelope.get("payload") or {}).get("eventId") or envelope.get("ingestion_id")

    if dedup.is_duplicate(event_id):
        log.debug("Duplicate event_id=%s — skipping", event_id)
        return

    try:
        record = transform(envelope)
    except Exception as exc:
        send_to_dlq(dlq_producer, envelope, f"transform_error: {exc}")
        return

    if record is None:  # unknown event type
        send_to_dlq(dlq_producer, envelope, "unknown_event_type")
        return

    batch.append(record)


def _commit(consumer: Consumer):
    """
    Tell Kafka we're done with everything consumed so far. asynchronous=False
    makes this a blocking call so we know the commit actually happened.
    """
    try:
        consumer.commit(asynchronous=False)
    except Exception as exc:
        # "_NO_OFFSET" just means there was nothing new to commit.
        if "_NO_OFFSET" not in str(exc):
            log.warning("Offset commit failed: %s", exc)


def _flush(batch: list[dict], writer: DBWriter, dlq_producer: Producer):
    """
    Write the batch to the database with retries.

    Exponential backoff: wait 1s, 2.5s, 5.5s, ... between attempts. If
    the database is down, hammering it with instant retries only makes
    recovery harder — backing off gives it room to come back.

    After 5 failed attempts we dead-letter the whole batch rather than
    blocking forever. Kafka keeps buffering new events meanwhile (that's
    the whole point of putting a broker between ingestion and the DB).
    """
    backoff = 1.0
    for attempt in range(5):
        try:
            written = writer.write_batch(batch)
            log.info("Flushed batch of %d (wrote %d)", len(batch), written)
            return
        except Exception as exc:
            log.warning("DB write attempt %d failed: %s", attempt + 1, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2 + 0.5, 30)

    log.error("Exhausted retries — sending %d records to DLQ", len(batch))
    for record in batch:
        send_to_dlq(dlq_producer, record, "db_write_exhausted")


if __name__ == "__main__":
    run()
