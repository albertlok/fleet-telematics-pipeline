"""
Redis-backed idempotency (deduplication) cache.

THE PROBLEM: webhook providers retry deliveries when they don't get a
fast 200 OK — a network blip can make the same event arrive twice. Our
own ingestion layer can also produce duplicates during Kafka retries.
Without protection we'd write the same row to the database twice.

THE FIX: before processing an event, atomically record its event_id in
Redis. If the id was already there, we've seen this event before — skip it.

Why Redis and not the database? This check happens for EVERY event, so
it must be fast. A Redis lookup is sub-millisecond and doesn't add load
to the database we're trying to protect.

Keys expire after 48 hours (the TTL). That's long enough to cover any
realistic provider retry window, and expiry keeps Redis memory bounded —
we don't need to remember events forever, only long enough to catch
retries of them.
"""

import logging

import redis

log = logging.getLogger(__name__)

_TTL_SECONDS = 48 * 3600  # 48 hours


class DeduplicationCache:
    def __init__(self, redis_url: str):
        # decode_responses=True makes the client return Python strings
        # instead of raw bytes.
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)

    def is_duplicate(self, event_id: str) -> bool:
        """
        Return True if this event_id has already been seen.

        This uses a single atomic Redis command: SET key value NX EX ttl.
          - NX = "only set if the key does Not eXist"
          - EX = set the expiry (TTL) in seconds

        Redis returns True if it created the key (first time we've seen
        this event) and None if the key already existed (duplicate).
        Doing the check and the write in ONE atomic command matters:
        a separate "check then set" would let two processor pods racing
        on the same event both conclude it's new.
        """
        key = f"telemetry:dedup:{event_id}"
        inserted = self._client.set(key, "1", ex=_TTL_SECONDS, nx=True)
        return inserted is None  # None → key already existed → duplicate

    def close(self):
        self._client.close()
