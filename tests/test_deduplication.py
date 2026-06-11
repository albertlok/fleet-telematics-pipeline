"""
Unit tests for the deduplication cache.

fakeredis provides an in-memory Redis lookalike, so the real dedup logic
(atomic SET NX EX) runs unchanged — just against fake storage instead of
a live server.
"""

from unittest.mock import patch

import fakeredis
import pytest

from deduplication import DeduplicationCache


@pytest.fixture()
def cache():
    """A DeduplicationCache wired to an in-memory fake Redis."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    with patch("deduplication.redis.Redis") as mock_redis:
        mock_redis.from_url.return_value = fake
        yield DeduplicationCache("redis://localhost:6379/0")


def test_first_event_is_not_duplicate(cache):
    assert cache.is_duplicate("evt-abc-001") is False


def test_second_occurrence_is_duplicate(cache):
    cache.is_duplicate("evt-abc-002")  # first sighting: records it
    assert cache.is_duplicate("evt-abc-002") is True


def test_different_event_ids_are_independent(cache):
    cache.is_duplicate("evt-x")
    assert cache.is_duplicate("evt-y") is False


def test_keys_carry_a_ttl(cache):
    """Every dedup key must expire, or Redis memory would grow forever."""
    cache.is_duplicate("evt-ttl-check")
    ttl = cache._client.ttl("telemetry:dedup:evt-ttl-check")
    assert 0 < ttl <= 48 * 3600
